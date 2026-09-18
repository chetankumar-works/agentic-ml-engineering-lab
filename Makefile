.PHONY: install lint fmt typecheck test up down logs migrate seed smoke

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
