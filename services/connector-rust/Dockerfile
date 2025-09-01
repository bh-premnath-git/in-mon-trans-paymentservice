FROM rust:1.83-slim

WORKDIR /usr/src/app

# Tools for healthchecks & TLS
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash netcat-openbsd ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create a minimal project if none is mounted yet
RUN USER=root cargo new --bin connector
WORKDIR /usr/src/app/connector

# Minimal deps: tonic + tonic-health + tracing
RUN cat > Cargo.toml <<'CARGO'
[package]
name = "connector"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1.37", features = ["macros", "rt-multi-thread"] }
tonic = { version = "0.11", features = ["transport"] }
tonic-health = "0.11"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["fmt", "env-filter"] }
CARGO

RUN mkdir -p src
RUN cat > src/main.rs <<'RS'
use tonic::transport::Server;
use tonic_health::server::health_reporter; // 0.11 API
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let addr = format!("0.0.0.0:{}", std::env::var("GRPC_PORT").unwrap_or("50051".into()))
        .parse()?;

    // health_reporter returns (HealthReporter, HealthServer).
    // For now (no app service yet), we dont mark any specific service as SERVING.
    let (_reporter, health_svc) = health_reporter();

    tracing::info!("connector (health-only) listening on {}", addr);
    Server::builder()
        .add_service(health_svc)
        .serve(addr)
        .await?;

    Ok(())
}
RS

# Dev hot-reload
RUN cargo install cargo-watch --locked

ENV GRPC_PORT=50051 RUST_LOG=info
EXPOSE 50051

# Watch + rebuild on file changes; good for local dev
CMD ["cargo", "watch", "-x", "run"]
