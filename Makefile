SHELL := /bin/bash
COMPOSE := docker compose
ENV_FILE ?= .env

.PHONY: help
help:
	@echo "Targets:"
	@echo "  build   - Build all images"
	@echo "  up      - Start stack in background"
	@echo "  down    - Stop stack"
	@echo "  restart - Recreate stack"
	@echo "  logs    - Follow logs"
	@echo "  ps      - Show status"
	@echo "  ping    - Quick health pings"

.PHONY: build
build:
	$(COMPOSE) --env-file $(ENV_FILE) build

.PHONY: up
up:
	$(COMPOSE) --env-file $(ENV_FILE) up -d

.PHONY: down
down:
	$(COMPOSE) --env-file $(ENV_FILE) down

.PHONY: restart
restart:
	$(COMPOSE) --env-file $(ENV_FILE) down
	$(COMPOSE) --env-file $(ENV_FILE) up -d

.PHONY: logs
logs:
	$(COMPOSE) --env-file $(ENV_FILE) logs -f --tail=200

.PHONY: ps
ps:
	$(COMPOSE) --env-file $(ENV_FILE) ps

.PHONY: ping
ping:
	@echo "FastAPI -> http://localhost:8000/healthz"
	@curl -sf http://localhost:8000/healthz && echo "  OK" || (echo "  FAIL" && exit 1)
	@echo "gRPC (TCP) -> localhost:50051"
	@bash -lc 'echo > /dev/tcp/127.0.0.1/50051' 2>/dev/null && echo "  OK" || (echo "  FAIL" && exit 1)
