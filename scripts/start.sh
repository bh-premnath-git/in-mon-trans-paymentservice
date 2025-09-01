#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE="${ENV_FILE:-.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "No .env found – creating a basic one."
  cat > .env <<'EOF'
POSTGRES_USER=payments
POSTGRES_PASSWORD=payments
POSTGRES_DB=payments

APP_PORT=8000
GRPC_PORT=50051
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317
EOF
fi

echo "▶️  Building images…"
docker compose --env-file "$ENV_FILE" build

echo "▶️  Starting services…"
docker compose --env-file "$ENV_FILE" up -d

echo "⏳ Waiting for services to be healthy…"
attempts=0
until curl -sf "http://localhost:${APP_PORT:-8000}/healthz" >/dev/null 2>&1; do
  attempts=$((attempts+1))
  if [[ $attempts -gt 60 ]]; then echo "payments-python failed to get healthy"; exit 1; fi
  sleep 1
done

# simple TCP check for gRPC port
if ! bash -lc 'echo > /dev/tcp/127.0.0.1/'"${GRPC_PORT:-50051}" 2>/dev/null; then
  echo "connector-rust not listening on ${GRPC_PORT:-50051}"; exit 1
fi

echo "✅ Up!"
echo "  FastAPI: http://localhost:${APP_PORT:-8000}/docs  (healthz at /healthz)"
echo "  Jaeger:  http://localhost:16686"
echo "  Kafka:   PLAINTEXT at localhost:9092 (Redpanda)"
echo "  gRPC:    connector at localhost:${GRPC_PORT:-50051}"
