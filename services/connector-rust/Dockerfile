FROM rust:1.83-slim

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash netcat-openbsd ca-certificates protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo install cargo-watch --locked

ENV GRPC_PORT=50051 RUST_LOG=info
EXPOSE 50051

CMD ["cargo", "watch", "-x", "run"]
