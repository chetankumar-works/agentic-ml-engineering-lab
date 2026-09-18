.PHONY: install lint fmt typecheck test up down logs migrate seed smoke

# --- Working targets ---

install:
	uv sync

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

typecheck:
	uv run mypy .

test:
	uv run pytest

# --- Infra-dependent targets (land starting Milestone 1) ---
# These intentionally exit 0 so `make` never errors on a target that
# doesn't have infra behind it yet — see PROJECT_STATE.md for status.

up:
	@echo "not yet implemented: docker compose stack arrives in Milestone 1"

down:
	@echo "not yet implemented: docker compose stack arrives in Milestone 1"

logs:
	@echo "not yet implemented: docker compose stack arrives in Milestone 1"

migrate:
	@echo "not yet implemented: Alembic migrations arrive in Milestone 1"

seed:
	@echo "not yet implemented: seed data arrives in Milestone 1"

smoke:
	@echo "not yet implemented: smoke tests arrive in Milestone 1"
