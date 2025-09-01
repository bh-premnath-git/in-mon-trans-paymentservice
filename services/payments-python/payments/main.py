# ============================================
# PYTHON ORCHESTRATION SERVICE
# ============================================

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum, auto
from typing import Optional, Dict, List, Any, Callable
import asyncio
import json
import uuid
from contextlib import asynccontextmanager

import grpc
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
import asyncpg
from redis import asyncio as aioredis
import structlog
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient
from transitions import State, StateMachine

# Import generated gRPC stubs
from .pb import payments_pb2 as payment_pb2
from .pb import payments_pb2_grpc as payment_pb2_grpc

# ============================================
# CONFIGURATION
# ============================================

class Config:
    POSTGRES_DSN = "postgresql://user:pass@localhost/payments"
    REDIS_URL = "redis://localhost:6379"
    GRPC_CONNECTOR_URL = "localhost:50051"
    
    # Idempotency
    IDEMPOTENCY_KEY_TTL = 86400  # 24 hours
    
    # Circuit breaker
    CIRCUIT_FAILURE_THRESHOLD = 0.5
    CIRCUIT_TIMEOUT_MS = 30000
    
    # Retry policy
    MAX_RETRIES = 3
    RETRY_BACKOFF_MS = [100, 500, 2000]
    
    # Webhook URLs
    WEBHOOK_TIMEOUT = 10
    WEBHOOK_MAX_RETRIES = 5

config = Config()

# ============================================
# LOGGING & TRACING
# ============================================

logger = structlog.get_logger()
tracer = trace.get_tracer(__name__)

# ============================================
# DOMAIN MODELS
# ============================================

@dataclass
class Money:
    """Immutable money value object with proper decimal handling."""
    amount: Decimal  # Always in minor units (cents)
    currency: str    # ISO-4217
    
    def __post_init__(self):
        if not isinstance(self.amount, Decimal):
            self.amount = Decimal(str(self.amount))
        if not self.currency or len(self.currency) != 3:
            raise ValueError(f"Invalid currency: {self.currency}")
        self.currency = self.currency.upper()
    
    @property
    def minor_units(self) -> int:
        """Get number of decimal places for currency."""
        units = {
            'JPY': 0, 'KRW': 0,
            'BHD': 3, 'KWD': 3, 'OMR': 3,
        }
        return units.get(self.currency, 2)
    
    def to_major_units(self) -> Decimal:
        """Convert to major currency units."""
        divisor = Decimal(10 ** self.minor_units)
        return (self.amount / divisor).quantize(
            Decimal(f'0.{"0" * self.minor_units}'),
            rounding=ROUND_HALF_UP
        )
    
    def __add__(self, other: 'Money') -> 'Money':
        if self.currency != other.currency:
            raise ValueError("Currency mismatch")
        return Money(self.amount + other.amount, self.currency)

class PaymentStatus(Enum):
    REQUESTED = auto()
    AUTHORIZED = auto()
    CAPTURED = auto()
    SETTLED = auto()
    FAILED = auto()
    REFUNDED = auto()
    PARTIALLY_REFUNDED = auto()
    CANCELLED = auto()

@dataclass
class PaymentIntent:
    id: str
    tenant_id: str
    idempotency_key: str
    amount: Money
    status: PaymentStatus
    provider: Optional[str] = None
    provider_reference: Optional[str] = None
    customer_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    authorized_at: Optional[datetime] = None
    captured_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    failure_reason: Optional[str] = None
    refunds: List['Refund'] = field(default_factory=list)

@dataclass
class Refund:
    id: str
    payment_id: str
    amount: Money
    status: PaymentStatus
    reason: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)

# ============================================
# STATE MACHINE
# ============================================

class PaymentStateMachine(StateMachine):
    """State machine for payment lifecycle with SAGA pattern."""
    
    # States
    requested = State(initial=True)
    authorizing = State()
    authorized = State()
    capturing = State()
    captured = State()
    settling = State()
    settled = State()
    failing = State()
    failed = State()
    refunding = State()
    refunded = State()
    partially_refunded = State()
    
    # Transitions
    start_authorize = requested.to(authorizing)
    authorize_success = authorizing.to(authorized)
    authorize_failure = authorizing.to(failing)
    
    start_capture = authorized.to(capturing)
    capture_success = capturing.to(captured)
    capture_failure = capturing.to(failing)
    
    start_settle = captured.to(settling)
    settle_success = settling.to(settled)
    settle_failure = settling.to(failing)
    
    start_refund = (captured | settled).to(refunding)
    refund_success = refunding.to(refunded)
    partial_refund_success = refunding.to(partially_refunded)
    refund_failure = refunding.to(failing)
    
    finalize_failure = failing.to(failed)
    
    def __init__(self, payment_intent: PaymentIntent):
        self.payment_intent = payment_intent
        super().__init__()
    
    def on_enter_authorizing(self):
        logger.info("Starting authorization", payment_id=self.payment_intent.id)
    
    def on_enter_authorized(self):
        self.payment_intent.status = PaymentStatus.AUTHORIZED
        self.payment_intent.authorized_at = datetime.utcnow()
        logger.info("Payment authorized", payment_id=self.payment_intent.id)
    
    def on_enter_captured(self):
        self.payment_intent.status = PaymentStatus.CAPTURED
        self.payment_intent.captured_at = datetime.utcnow()
        logger.info("Payment captured", payment_id=self.payment_intent.id)
    
    def on_enter_failed(self):
        self.payment_intent.status = PaymentStatus.FAILED
        self.payment_intent.failed_at = datetime.utcnow()
        logger.error("Payment failed", 
                    payment_id=self.payment_intent.id,
                    reason=self.payment_intent.failure_reason)

# ============================================
# OUTBOX PATTERN FOR EVENT PUBLISHING
# ============================================

@dataclass
class OutboxEvent:
    id: str
    aggregate_id: str
    event_type: str
    payload: Dict[str, Any]
    created_at: datetime = field(default_factory=datetime.utcnow)
    published: bool = False
    published_at: Optional[datetime] = None

class EventOutbox:
    """Transactional outbox for exactly-once event delivery."""
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool
    
    async def publish(self, conn: asyncpg.Connection, event: OutboxEvent):
        """Store event in outbox within transaction."""
        await conn.execute("""
            INSERT INTO payment_events (id, aggregate_id, event_type, payload, created_at)
            VALUES ($1, $2, $3, $4, $5)
        """, event.id, event.aggregate_id, event.event_type, 
            json.dumps(event.payload), event.created_at)
    
    async def get_unpublished(self, limit: int = 100) -> List[OutboxEvent]:
        """Fetch unpublished events for processing."""
        async with self.db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM payment_events 
                WHERE published = false 
                ORDER BY created_at 
                LIMIT $1
            """, limit)
            
            return [OutboxEvent(
                id=row['id'],
                aggregate_id=row['aggregate_id'],
                event_type=row['event_type'],
                payload=json.loads(row['payload']),
                created_at=row['created_at'],
                published=row['published']
            ) for row in rows]
    
    async def mark_published(self, event_ids: List[str]):
        """Mark events as published."""
        async with self.db.acquire() as conn:
            await conn.execute("""
                UPDATE payment_events 
                SET published = true, published_at = NOW() 
                WHERE id = ANY($1)
            """, event_ids)

# ============================================
# RULES ENGINE
# ============================================

class RuleCondition:
    """Base class for rule conditions."""
    
    def evaluate(self, context: Dict[str, Any]) -> bool:
        raise NotImplementedError

class AmountThreshold(RuleCondition):
    def __init__(self, threshold: Decimal, operator: str = 'gt'):
        self.threshold = threshold
        self.operator = operator
    
    def evaluate(self, context: Dict[str, Any]) -> bool:
        amount = context.get('amount', 0)
        if self.operator == 'gt':
            return amount > self.threshold
        elif self.operator == 'lt':
            return amount < self.threshold
        elif self.operator == 'eq':
            return amount == self.threshold
        return False

class CountryRule(RuleCondition):
    def __init__(self, countries: List[str], allow: bool = True):
        self.countries = set(countries)
        self.allow = allow
    
    def evaluate(self, context: Dict[str, Any]) -> bool:
        country = context.get('country', '')
        in_list = country in self.countries
        return in_list if self.allow else not in_list

class RiskScoreRule(RuleCondition):
    def __init__(self, max_score: int):
        self.max_score = max_score
    
    def evaluate(self, context: Dict[str, Any]) -> bool:
        score = context.get('risk_score', 0)
        return score <= self.max_score

@dataclass
class Rule:
    id: str
    name: str
    conditions: List[RuleCondition]
    action: str  # 'approve', 'decline', 'review', '3ds_required'
    priority: int = 0
    enabled: bool = True

class RulesEngine:
    """Business rules engine for payment processing decisions."""
    
    def __init__(self):
        self.rules: List[Rule] = []
        self._load_default_rules()
    
    def _load_default_rules(self):
        """Load default ruleset."""
        self.rules = [
            Rule(
                id="high_amount_3ds",
                name="Require 3DS for high amounts",
                conditions=[AmountThreshold(Decimal(10000), 'gt')],  # >$100
                action="3ds_required",
                priority=10
            ),
            Rule(
                id="high_risk_decline",
                name="Decline high risk transactions",
                conditions=[RiskScoreRule(max_score=20)],
                action="decline",
                priority=20
            ),
            Rule(
                id="blocked_countries",
                name="Block sanctioned countries",
                conditions=[CountryRule(['KP', 'IR', 'SY'], allow=False)],
                action="decline",
                priority=30
            ),
        ]
        self.rules.sort(key=lambda r: r.priority)
    
    def evaluate(self, context: Dict[str, Any]) -> Optional[str]:
        """Evaluate rules and return action."""
        for rule in self.rules:
            if not rule.enabled:
                continue
            
            if all(cond.evaluate(context) for cond in rule.conditions):
                logger.info("Rule matched", rule_id=rule.id, action=rule.action)
                return rule.action
        
        return "approve"  # Default action

# ============================================
# IDEMPOTENCY HANDLER
# ============================================

class IdempotencyHandler:
    """Handle idempotent requests using Redis."""
    
    def __init__(self, redis: aioredis.Redis):
        self.redis = redis
    
    def _key(self, tenant_id: str, idempotency_key: str) -> str:
        """Generate cache key."""
        return f"idempotency:{tenant_id}:{idempotency_key}"
    
    async def get_cached_response(self, tenant_id: str, idempotency_key: str) -> Optional[Dict]:
        """Get cached response if exists."""
        key = self._key(tenant_id, idempotency_key)
        data = await self.redis.get(key)
        if data:
            return json.loads(data)
        return None
    
    async def cache_response(self, tenant_id: str, idempotency_key: str, 
                           response: Dict, ttl: int = 86400):
        """Cache response with TTL."""
        key = self._key(tenant_id, idempotency_key)
        await self.redis.setex(key, ttl, json.dumps(response))

# ============================================
# PAYMENT SERVICE
# ============================================

class PaymentService:
    """Core payment orchestration service."""
    
    def __init__(self, db_pool: asyncpg.Pool, redis: aioredis.Redis):
        self.db = db_pool
        self.redis = redis
        self.idempotency = IdempotencyHandler(redis)
        self.outbox = EventOutbox(db_pool)
        self.rules_engine = RulesEngine()
        self.grpc_channel = None
        self.grpc_stub = None
        self._init_grpc()
    
    def _init_grpc(self):
        """Initialize gRPC connection to Rust connector."""
        self.grpc_channel = grpc.aio.insecure_channel(
            config.GRPC_CONNECTOR_URL,
            options=[
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
            ]
        )
        self.grpc_stub = payment_pb2_grpc.PaymentServiceStub(self.grpc_channel)
        GrpcInstrumentorClient().instrument()
    
    async def create_payment(self, request: 'CreatePaymentRequest') -> PaymentIntent:
        """Create a new payment with idempotency."""
        
        # Check idempotency
        cached = await self.idempotency.get_cached_response(
            request.tenant_id, request.idempotency_key
        )
        if cached:
            return PaymentIntent(**cached)
        
        # Create payment intent
        payment = PaymentIntent(
            id=str(uuid.uuid4()),
            tenant_id=request.tenant_id,
            idempotency_key=request.idempotency_key,
            amount=Money(request.amount, request.currency),
            status=PaymentStatus.REQUESTED,
            customer_id=request.customer_id,
            metadata=request.metadata or {}
        )
        
        # Evaluate rules
        rule_context = {
            'amount': payment.amount.amount,
            'currency': payment.amount.currency,
            'country': request.country,
            'risk_score': request.metadata.get('risk_score', 0) if request.metadata else 0
        }
        action = self.rules_engine.evaluate(rule_context)
        
        if action == 'decline':
            payment.status = PaymentStatus.FAILED
            payment.failure_reason = "Declined by rules engine"
        elif action == '3ds_required':
            payment.metadata['3ds_required'] = True
        
        # Save to database
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await self._save_payment(conn, payment)
                
                # Publish event
                event = OutboxEvent(
                    id=str(uuid.uuid4()),
                    aggregate_id=payment.id,
                    event_type='payment.created',
                    payload=self._payment_to_dict(payment)
                )
                await self.outbox.publish(conn, event)
        
        # Cache response
        await self.idempotency.cache_response(
            request.tenant_id, 
            request.idempotency_key,
            self._payment_to_dict(payment)
        )
        
        return payment
    
    async def authorize_payment(self, payment_id: str) -> PaymentIntent:
        """Authorize payment through connector."""
        payment = await self._get_payment(payment_id)
        
        if payment.status != PaymentStatus.REQUESTED:
            raise ValueError(f"Invalid state for authorization: {payment.status}")
        
        # Create state machine
        sm = PaymentStateMachine(payment)
        sm.start_authorize()
        
        try:
            # Call Rust connector via gRPC
            request = payment_pb2.AuthorizeRequest(
                idempotency_key=payment.idempotency_key,
                tenant_id=payment.tenant_id,
                amount=int(payment.amount.amount),
                currency=payment.amount.currency,
                metadata=payment.metadata
            )
            
            with tracer.start_as_current_span("grpc.authorize"):
                response = await self.grpc_stub.Authorize(request)
            
            payment.provider = response.provider
            payment.provider_reference = response.provider_reference
            sm.authorize_success()
            
        except Exception as e:
            payment.failure_reason = str(e)
            sm.authorize_failure()
            sm.finalize_failure()
            logger.error("Authorization failed", error=str(e), payment_id=payment_id)
            raise
        
        # Save state
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await self._save_payment(conn, payment)
                
                # Publish event
                event = OutboxEvent(
                    id=str(uuid.uuid4()),
                    aggregate_id=payment.id,
                    event_type='payment.authorized',
                    payload=self._payment_to_dict(payment)
                )
                await self.outbox.publish(conn, event)
        
        return payment
    
    async def capture_payment(self, payment_id: str, amount: Optional[Money] = None) -> PaymentIntent:
        """Capture authorized payment."""
        payment = await self._get_payment(payment_id)
        
        if payment.status != PaymentStatus.AUTHORIZED:
            raise ValueError(f"Invalid state for capture: {payment.status}")
        
        sm = PaymentStateMachine(payment)
        sm.start_capture()
        
        try:
            request = payment_pb2.CaptureRequest(
                payment_id=payment.id,
                provider_reference=payment.provider_reference
            )
            if amount:
                request.amount = int(amount.amount)
            
            with tracer.start_as_current_span("grpc.capture"):
                response = await self.grpc_stub.Capture(request)
            
            sm.capture_success()
            
        except Exception as e:
            payment.failure_reason = str(e)
            sm.capture_failure()
            sm.finalize_failure()
            logger.error("Capture failed", error=str(e), payment_id=payment_id)
            raise
        
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await self._save_payment(conn, payment)
                
                event = OutboxEvent(
                    id=str(uuid.uuid4()),
                    aggregate_id=payment.id,
                    event_type='payment.captured',
                    payload=self._payment_to_dict(payment)
                )
                await self.outbox.publish(conn, event)
        
        return payment
    
    async def _get_payment(self, payment_id: str) -> PaymentIntent:
        """Load payment from database."""
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM payments WHERE id = $1",
                payment_id
            )
            if not row:
                raise ValueError(f"Payment not found: {payment_id}")
            
            return self._row_to_payment(row)
    
    async def _save_payment(self, conn: asyncpg.Connection, payment: PaymentIntent):
        """Save payment to database."""
        await conn.execute("""
            INSERT INTO payments (
                id, tenant_id, idempotency_key, amount, currency, 
                status, provider, provider_reference, customer_id,
                metadata, created_at, updated_at, authorized_at,
                captured_at, failed_at, failure_reason
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (id) DO UPDATE SET
                status = $6, provider = $7, provider_reference = $8,
                metadata = $10, updated_at = $12, authorized_at = $13,
                captured_at = $14, failed_at = $15, failure_reason = $16
        """, payment.id, payment.tenant_id, payment.idempotency_key,
            int(payment.amount.amount), payment.amount.currency,
            payment.status.name, payment.provider, payment.provider_reference,
            payment.customer_id, json.dumps(payment.metadata),
            payment.created_at, datetime.utcnow(), payment.authorized_at,
            payment.captured_at, payment.failed_at, payment.failure_reason)
    
    def _payment_to_dict(self, payment: PaymentIntent) -> Dict:
        """Convert payment to dictionary."""
        return {
            'id': payment.id,
            'tenant_id': payment.tenant_id,
            'amount': str(payment.amount.amount),
            'currency': payment.amount.currency,
            'status': payment.status.name,
            'provider': payment.provider,
            'provider_reference': payment.provider_reference,
            'metadata': payment.metadata,
            'created_at': payment.created_at.isoformat(),
        }
    
    def _row_to_payment(self, row) -> PaymentIntent:
        """Convert database row to PaymentIntent."""
        return PaymentIntent(
            id=row['id'],
            tenant_id=row['tenant_id'],
            idempotency_key=row['idempotency_key'],
            amount=Money(Decimal(row['amount']), row['currency']),
            status=PaymentStatus[row['status']],
            provider=row['provider'],
            provider_reference=row['provider_reference'],
            customer_id=row['customer_id'],
            metadata=json.loads(row['metadata']) if row['metadata'] else {},
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            authorized_at=row['authorized_at'],
            captured_at=row['captured_at'],
            failed_at=row['failed_at'],
            failure_reason=row['failure_reason']
        )

# ============================================
# REST API
# ============================================

app = FastAPI(title="Payment Orchestration API")

# Dependency injection
async def get_db():
    return app.state.db_pool

async def get_redis():
    return app.state.redis

async def get_payment_service():
    return app.state.payment_service

# Request/Response models
class CreatePaymentRequest(BaseModel):
    tenant_id: str
    idempotency_key: str
    amount: int  # Minor units
    currency: str
    customer_id: Optional[str] = None
    country: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class PaymentResponse(BaseModel):
    id: str
    status: str
    amount: str
    currency: str
    provider: Optional[str]
    provider_reference: Optional[str]
    created_at: str

# API Endpoints
@app.post("/payments", response_model=PaymentResponse)
async def create_payment(
    request: CreatePaymentRequest,
    service: PaymentService = Depends(get_payment_service)
):
    """Create a new payment."""
    with tracer.start_as_current_span("create_payment"):
        payment = await service.create_payment(request)
        return PaymentResponse(
            id=payment.id,
            status=payment.status.name,
            amount=str(payment.amount.amount),
            currency=payment.amount.currency,
            provider=payment.provider,
            provider_reference=payment.provider_reference,
            created_at=payment.created_at.isoformat()
        )

@app.post("/payments/{payment_id}/authorize")
async def authorize_payment(
    payment_id: str,
    service: PaymentService = Depends(get_payment_service)
):
    """Authorize a payment."""
    with tracer.start_as_current_span("authorize_payment"):
        payment = await service.authorize_payment(payment_id)
        return PaymentResponse(
            id=payment.id,
            status=payment.status.name,
            amount=str(payment.amount.amount),
            currency=payment.amount.currency,
            provider=payment.provider,
            provider_reference=payment.provider_reference,
            created_at=payment.created_at.isoformat()
        )

@app.post("/payments/{payment_id}/capture")
async def capture_payment(
    payment_id: str,
    amount: Optional[int] = None,
    service: PaymentService = Depends(get_payment_service)
):
    """Capture an authorized payment."""
    with tracer.start_as_current_span("capture_payment"):
        capture_amount = None
        if amount:
            # Get payment to get currency
            payment = await service._get_payment(payment_id)
            capture_amount = Money(Decimal(amount), payment.amount.currency)
        
        payment = await service.capture_payment(payment_id, capture_amount)
        return PaymentResponse(
            id=payment.id,
            status=payment.status.name,
            amount=str(payment.amount.amount),
            currency=payment.amount.currency,
            provider=payment.provider,
            provider_reference=payment.provider_reference,
            created_at=payment.created_at.isoformat()
        )

@app.post("/webhooks/stripe")
async def stripe_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    service: PaymentService = Depends(get_payment_service)
):
    """Handle Stripe webhooks."""
    payload = await request.body()
    signature = request.headers.get('Stripe-Signature')
    
    # Verify webhook signature (implement verification)
    # Process webhook asynchronously
    background_tasks.add_task(process_stripe_webhook, payload, service)
    
    return {"status": "accepted"}

async def process_stripe_webhook(payload: bytes, service: PaymentService):
    """Process Stripe webhook in background."""
    # Parse and process webhook
    pass

# Application lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app.state.db_pool = await asyncpg.create_pool(config.POSTGRES_DSN)
    app.state.redis = await aioredis.from_url(config.REDIS_URL)
    app.state.payment_service = PaymentService(app.state.db_pool, app.state.redis)
    
    # Start background tasks
    asyncio.create_task(process_outbox_events(app.state.payment_service))
    
    yield
    
    # Shutdown
    await app.state.db_pool.close()
    await app.state.redis.close()

app.router.lifespan_context = lifespan

async def process_outbox_events(service: PaymentService):
    """Background task to process outbox events."""
    while True:
        try:
            events = await service.outbox.get_unpublished(limit=100)
            if events:
                # Publish to Kafka/SQS/etc
                # await publish_to_kafka(events)
                await service.outbox.mark_published([e.id for e in events])
        except Exception as e:
            logger.error("Outbox processing error", error=str(e))
        
        await asyncio.sleep(5)

# Instrumentation
FastAPIInstrumentor.instrument_app(app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
