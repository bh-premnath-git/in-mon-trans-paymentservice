// ============================================
// RUST CONNECTOR SERVICE - src/main.rs
// ============================================

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use uuid::Uuid;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};

// ============================================
// DOMAIN MODELS
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Money {
    amount: i64,  // Minor units (cents)
    currency: String,  // ISO-4217
}

impl Money {
    pub fn new(amount: i64, currency: &str) -> Result<Self, String> {
        if !currency.chars().all(|c| c.is_ascii_uppercase()) || currency.len() != 3 {
            return Err(format!("Invalid currency: {}", currency));
        }
        Ok(Money {
            amount,
            currency: currency.to_string(),
        })
    }

    pub fn to_decimal(&self) -> BigDecimal {
        let minor_units = self.minor_units();
        BigDecimal::from(self.amount) / BigDecimal::from(10_i64.pow(minor_units))
    }

    fn minor_units(&self) -> u32 {
        match self.currency.as_str() {
            "JPY" | "KRW" => 0,
            "BHD" | "KWD" | "OMR" => 3,
            _ => 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PaymentState {
    Requested,
    Authorized,
    Captured,
    Settled,
    Failed,
    Refunded,
    PartiallyRefunded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentIntent {
    pub id: String,
    pub idempotency_key: String,
    pub tenant_id: String,
    pub amount: Money,
    pub state: PaymentState,
    pub provider: Option<String>,
    pub provider_reference: Option<String>,
    pub metadata: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ============================================
// PROVIDER TRAIT & CAPABILITIES
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub card_present: bool,
    pub card_not_present: bool,
    pub bank_transfer: bool,
    pub digital_wallet: bool,
    pub currencies: Vec<String>,
    pub min_amount: HashMap<String, i64>,
    pub max_amount: HashMap<String, i64>,
    pub requires_3ds: bool,
    pub supports_partial_refund: bool,
}

#[derive(Debug, Clone)]
pub struct ProviderHealth {
    pub success_rate: f64,
    pub p99_latency_ms: u64,
    pub circuit_state: CircuitState,
    pub last_check: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[async_trait]
pub trait PaymentProvider: Send + Sync {
    fn name(&self) -> &str;
    fn capabilities(&self) -> &ProviderCapabilities;
    
    async fn authorize(&self, intent: &PaymentIntent) -> Result<String, ProviderError>;
    async fn capture(&self, intent: &PaymentIntent, amount: Option<Money>) -> Result<String, ProviderError>;
    async fn refund(&self, intent: &PaymentIntent, amount: Option<Money>) -> Result<String, ProviderError>;
    async fn get_status(&self, provider_ref: &str) -> Result<PaymentState, ProviderError>;
}

#[derive(Debug)]
pub enum ProviderError {
    NetworkError(String),
    ValidationError(String),
    AuthenticationError(String),
    RateLimitError(String),
    ProcessingError(String),
    InvalidState(String),
}

// ============================================
// STRIPE ADAPTER
// ============================================

pub struct StripeAdapter {
    api_key: String,
    webhook_secret: String,
    client: reqwest::Client,
    capabilities: ProviderCapabilities,
}

impl StripeAdapter {
    pub fn new(api_key: String, webhook_secret: String) -> Self {
        let capabilities = ProviderCapabilities {
            card_present: true,
            card_not_present: true,
            bank_transfer: true,
            digital_wallet: true,
            currencies: vec!["USD", "EUR", "GBP", "JPY"].iter().map(|s| s.to_string()).collect(),
            min_amount: HashMap::from([
                ("USD".to_string(), 50),  // 50 cents
                ("EUR".to_string(), 50),
                ("GBP".to_string(), 30),
                ("JPY".to_string(), 50),
            ]),
            max_amount: HashMap::from([
                ("USD".to_string(), 999999900),  // $999,999
                ("EUR".to_string(), 999999900),
                ("GBP".to_string(), 999999900),
                ("JPY".to_string(), 99999999),
            ]),
            requires_3ds: true,
            supports_partial_refund: true,
        };

        Self {
            api_key,
            webhook_secret,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap(),
            capabilities,
        }
    }

    async fn make_request<T: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        body: Option<serde_json::Value>,
    ) -> Result<T, ProviderError> {
        let url = format!("https://api.stripe.com/v1/{}", endpoint);
        
        let mut request = self.client
            .request(method, &url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Stripe-Version", "2024-06-20");

        if let Some(b) = body {
            request = request.json(&b);
        }

        let response = request
            .send()
            .await
            .map_err(|e| ProviderError::NetworkError(e.to_string()))?;

        if !response.status().is_success() {
            let error_body = response.text().await.unwrap_or_default();
            return Err(ProviderError::ProcessingError(error_body));
        }

        response
            .json::<T>()
            .await
            .map_err(|e| ProviderError::ProcessingError(e.to_string()))
    }
}

#[async_trait]
impl PaymentProvider for StripeAdapter {
    fn name(&self) -> &str {
        "stripe"
    }

    fn capabilities(&self) -> &ProviderCapabilities {
        &self.capabilities
    }

    async fn authorize(&self, intent: &PaymentIntent) -> Result<String, ProviderError> {
        let body = serde_json::json!({
            "amount": intent.amount.amount,
            "currency": intent.amount.currency.to_lowercase(),
            "capture_method": "manual",
            "metadata": {
                "tenant_id": intent.tenant_id,
                "payment_id": intent.id,
            },
            "idempotency_key": intent.idempotency_key,
        });

        #[derive(Deserialize)]
        struct StripeResponse {
            id: String,
        }

        let response: StripeResponse = self
            .make_request(reqwest::Method::POST, "payment_intents", Some(body))
            .await?;

        Ok(response.id)
    }

    async fn capture(&self, intent: &PaymentIntent, amount: Option<Money>) -> Result<String, ProviderError> {
        let provider_ref = intent.provider_reference.as_ref()
            .ok_or_else(|| ProviderError::InvalidState("Missing provider reference".to_string()))?;

        let mut body = serde_json::json!({});
        if let Some(amt) = amount {
            body["amount_to_capture"] = serde_json::json!(amt.amount);
        }

        let endpoint = format!("payment_intents/{}/capture", provider_ref);
        
        #[derive(Deserialize)]
        struct StripeResponse {
            id: String,
        }

        let response: StripeResponse = self
            .make_request(reqwest::Method::POST, &endpoint, Some(body))
            .await?;

        Ok(response.id)
    }

    async fn refund(&self, intent: &PaymentIntent, amount: Option<Money>) -> Result<String, ProviderError> {
        let provider_ref = intent.provider_reference.as_ref()
            .ok_or_else(|| ProviderError::InvalidState("Missing provider reference".to_string()))?;

        let mut body = serde_json::json!({
            "payment_intent": provider_ref,
        });
        
        if let Some(amt) = amount {
            body["amount"] = serde_json::json!(amt.amount);
        }

        #[derive(Deserialize)]
        struct StripeResponse {
            id: String,
        }

        let response: StripeResponse = self
            .make_request(reqwest::Method::POST, "refunds", Some(body))
            .await?;

        Ok(response.id)
    }

    async fn get_status(&self, provider_ref: &str) -> Result<PaymentState, ProviderError> {
        #[derive(Deserialize)]
        struct StripePaymentIntent {
            status: String,
        }

        let endpoint = format!("payment_intents/{}", provider_ref);
        let response: StripePaymentIntent = self
            .make_request(reqwest::Method::GET, &endpoint, None)
            .await?;

        let state = match response.status.as_str() {
            "requires_payment_method" | "requires_confirmation" => PaymentState::Requested,
            "requires_action" | "processing" | "requires_capture" => PaymentState::Authorized,
            "succeeded" => PaymentState::Captured,
            "canceled" => PaymentState::Failed,
            _ => PaymentState::Failed,
        };

        Ok(state)
    }
}

// ============================================
// ROUTING STRATEGY
// ============================================

pub struct WeightedRoundRobinRouter {
    providers: Vec<(Arc<dyn PaymentProvider>, u32, Arc<RwLock<ProviderHealth>>)>,
    current_index: Arc<RwLock<usize>>,
}

impl WeightedRoundRobinRouter {
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
            current_index: Arc::new(RwLock::new(0)),
        }
    }

    pub fn add_provider(&mut self, provider: Arc<dyn PaymentProvider>, weight: u32) {
        let health = Arc::new(RwLock::new(ProviderHealth {
            success_rate: 1.0,
            p99_latency_ms: 0,
            circuit_state: CircuitState::Closed,
            last_check: Utc::now(),
        }));
        self.providers.push((provider, weight, health));
    }

    pub async fn select_provider(&self, intent: &PaymentIntent) -> Option<Arc<dyn PaymentProvider>> {
        let eligible_providers = self.filter_by_capabilities(intent).await;
        
        if eligible_providers.is_empty() {
            return None;
        }

        // Weighted selection with circuit breaker check
        let mut total_weight = 0u32;
        let mut available_providers = Vec::new();

        for (provider, weight, health) in &eligible_providers {
            let health_state = health.read().await;
            if health_state.circuit_state != CircuitState::Open {
                total_weight += weight;
                available_providers.push((provider.clone(), *weight));
            }
        }

        if available_providers.is_empty() {
            return None;
        }

        // Simple weighted round-robin
        let mut index = *self.current_index.write().await;
        index = (index + 1) % available_providers.len();
        *self.current_index.write().await = index;

        Some(available_providers[index].0.clone())
    }

    async fn filter_by_capabilities(&self, intent: &PaymentIntent) -> Vec<(Arc<dyn PaymentProvider>, u32, Arc<RwLock<ProviderHealth>>)> {
        let mut eligible = Vec::new();

        for (provider, weight, health) in &self.providers {
            let caps = provider.capabilities();
            
            // Check currency support
            if !caps.currencies.contains(&intent.amount.currency) {
                continue;
            }

            // Check amount limits
            if let Some(min) = caps.min_amount.get(&intent.amount.currency) {
                if intent.amount.amount < *min {
                    continue;
                }
            }

            if let Some(max) = caps.max_amount.get(&intent.amount.currency) {
                if intent.amount.amount > *max {
                    continue;
                }
            }

            eligible.push((provider.clone(), *weight, health.clone()));
        }

        eligible
    }
}

// ============================================
// CIRCUIT BREAKER
// ============================================

pub struct CircuitBreaker {
    failure_threshold: f64,
    success_threshold: f64,
    timeout_ms: u64,
    half_open_requests: u32,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self {
            failure_threshold: 0.5,  // 50% failure rate opens circuit
            success_threshold: 0.8,   // 80% success rate closes circuit
            timeout_ms: 30000,        // 30 seconds before trying half-open
            half_open_requests: 3,    // Test with 3 requests in half-open state
        }
    }

    pub async fn update_health(&self, health: &mut ProviderHealth, success: bool, latency_ms: u64) {
        // Update success rate (exponential moving average)
        let alpha = 0.1;  // Smoothing factor
        health.success_rate = if success {
            health.success_rate * (1.0 - alpha) + alpha
        } else {
            health.success_rate * (1.0 - alpha)
        };

        // Update P99 latency (simplified)
        health.p99_latency_ms = (health.p99_latency_ms * 99 + latency_ms) / 100;

        // State transitions
        match health.circuit_state {
            CircuitState::Closed => {
                if health.success_rate < self.failure_threshold {
                    health.circuit_state = CircuitState::Open;
                    health.last_check = Utc::now();
                }
            }
            CircuitState::Open => {
                let elapsed = (Utc::now() - health.last_check).num_milliseconds() as u64;
                if elapsed > self.timeout_ms {
                    health.circuit_state = CircuitState::HalfOpen;
                    health.last_check = Utc::now();
                }
            }
            CircuitState::HalfOpen => {
                if health.success_rate > self.success_threshold {
                    health.circuit_state = CircuitState::Closed;
                } else if health.success_rate < self.failure_threshold {
                    health.circuit_state = CircuitState::Open;
                    health.last_check = Utc::now();
                }
            }
        }
    }
}

// ============================================
// GRPC SERVICE IMPLEMENTATION
// ============================================

pub mod payment_service {
    tonic::include_proto!("payment");
}

use payment_service::{
    payment_service_server::{PaymentService, PaymentServiceServer},
    AuthorizeRequest, AuthorizeResponse,
    CaptureRequest, CaptureResponse,
    RefundRequest, RefundResponse,
};

pub struct PaymentServiceImpl {
    router: Arc<WeightedRoundRobinRouter>,
    circuit_breaker: Arc<CircuitBreaker>,
}

#[tonic::async_trait]
impl PaymentService for PaymentServiceImpl {
    async fn authorize(
        &self,
        request: Request<AuthorizeRequest>,
    ) -> Result<Response<AuthorizeResponse>, Status> {
        let req = request.into_inner();
        
        // Create payment intent
        let intent = PaymentIntent {
            id: Uuid::new_v4().to_string(),
            idempotency_key: req.idempotency_key,
            tenant_id: req.tenant_id,
            amount: Money::new(req.amount, &req.currency)
                .map_err(|e| Status::invalid_argument(e))?,
            state: PaymentState::Requested,
            provider: None,
            provider_reference: None,
            metadata: HashMap::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        // Select provider
        let provider = self.router
            .select_provider(&intent)
            .await
            .ok_or_else(|| Status::unavailable("No eligible provider available"))?;

        // Execute with circuit breaker tracking
        let start = std::time::Instant::now();
        let result = provider.authorize(&intent).await;
        let latency_ms = start.elapsed().as_millis() as u64;

        // Update health metrics
        let provider_name = provider.name();
        if let Some((_, _, health)) = self.router.providers.iter()
            .find(|(p, _, _)| p.name() == provider_name) {
            let mut health_state = health.write().await;
            self.circuit_breaker.update_health(&mut health_state, result.is_ok(), latency_ms).await;
        }

        let provider_ref = result.map_err(|e| Status::internal(format!("{:?}", e)))?;

        let response = AuthorizeResponse {
            payment_id: intent.id,
            provider: provider.name().to_string(),
            provider_reference: provider_ref,
            state: "authorized".to_string(),
        };

        Ok(Response::new(response))
    }

    async fn capture(
        &self,
        request: Request<CaptureRequest>,
    ) -> Result<Response<CaptureResponse>, Status> {
        // Implementation similar to authorize
        unimplemented!()
    }

    async fn refund(
        &self,
        request: Request<RefundRequest>,
    ) -> Result<Response<RefundResponse>, Status> {
        // Implementation similar to authorize
        unimplemented!()
    }
}

// ============================================
// WEBHOOK HANDLER
// ============================================

use hmac::{Hmac, Mac};
use sha2::Sha256;

pub struct WebhookVerifier {
    secrets: HashMap<String, String>,
}

impl WebhookVerifier {
    pub fn new() -> Self {
        Self {
            secrets: HashMap::new(),
        }
    }

    pub fn add_secret(&mut self, provider: &str, secret: &str) {
        self.secrets.insert(provider.to_string(), secret.to_string());
    }

    pub fn verify_stripe(&self, payload: &[u8], signature: &str) -> Result<(), String> {
        let secret = self.secrets.get("stripe")
            .ok_or_else(|| "Stripe webhook secret not configured".to_string())?;

        // Parse Stripe signature header
        let parts: HashMap<&str, &str> = signature
            .split(',')
            .filter_map(|part| {
                let mut split = part.trim().splitn(2, '=');
                Some((split.next()?, split.next()?))
            })
            .collect();

        let timestamp = parts.get("t")
            .ok_or_else(|| "Missing timestamp in signature".to_string())?;
        let signature_v1 = parts.get("v1")
            .ok_or_else(|| "Missing v1 signature".to_string())?;

        // Construct signed payload
        let signed_payload = format!("{}.{}", timestamp, std::str::from_utf8(payload).unwrap());

        // Compute expected signature
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .map_err(|e| format!("Invalid key: {}", e))?;
        mac.update(signed_payload.as_bytes());
        let expected = hex::encode(mac.finalize().into_bytes());

        if expected != *signature_v1 {
            return Err("Invalid signature".to_string());
        }

        // Check timestamp to prevent replay attacks (5 minute tolerance)
        let timestamp: i64 = timestamp.parse()
            .map_err(|_| "Invalid timestamp".to_string())?;
        let now = Utc::now().timestamp();
        if (now - timestamp).abs() > 300 {
            return Err("Timestamp too old".to_string());
        }

        Ok(())
    }
}

// ============================================
// MAIN SERVICE SETUP
// ============================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // Initialize providers
    let stripe = Arc::new(StripeAdapter::new(
        std::env::var("STRIPE_API_KEY")?,
        std::env::var("STRIPE_WEBHOOK_SECRET")?,
    ));

    // Setup router with weighted providers
    let mut router = WeightedRoundRobinRouter::new();
    router.add_provider(stripe.clone(), 70);  // Stripe gets 70% of traffic

    // Create gRPC service
    let service = PaymentServiceImpl {
        router: Arc::new(router),
        circuit_breaker: Arc::new(CircuitBreaker::new()),
    };

    let addr = "[::1]:50051".parse()?;
    
    println!("Payment connector service listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(PaymentServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
