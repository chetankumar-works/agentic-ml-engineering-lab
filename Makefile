.PHONY: install lint fmt typecheck test up down logs migrate seed smoke feast-materialize feast-demo failure-simulator-wedge

COMPOSE = docker compose -f infra/docker-compose.yml

install:
	uv sync --all-packages

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

typecheck:
	uv run mypy .

test:
	uv run pytest

# --- Infra-dependent targets (Milestone 1: Postgres + Kafka + source_simulator + stream_ingestor) ---

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

migrate:
	$(COMPOSE) run --rm migrate

seed:
	@echo "no separate seed step: landing data arrives continuously from source_simulator once 'make up' is running"

smoke:
	uv run python scripts/smoke_milestone1.py

# --- Feature store (Milestone 3: Feast + Redis) ---

feast-materialize:   # chunked backfill of curated -> Redis; resumes from the registry
	$(COMPOSE) --profile feast-demo run --rm --build feast-materialize

feast-demo:          # historical (point-in-time) + online retrieval acceptance demo
	$(COMPOSE) --profile feast-demo run --rm --build feast-demo

# --- Failure engineering (regression scripts for real incidents, see RUNBOOKS.md) ---

failure-simulator-wedge:   # pauses the Kafka broker; simulator probes must go 503 and recover
	uv run python scripts/failure_engineering/simulator_producer_wedge.py
