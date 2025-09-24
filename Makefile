.PHONY: install dev test uv-sync uv-test uv-python lint

# Default Python version for uv-managed environments
UV_PY ?= 3.12

# Prefer uv when available; fall back to pip/pytest
install:
	uv pip install -e . || python3 -m pip install --user -e .

dev:
	$(MAKE) uv-sync || python3 -m pip install --user -e ".[test]"

uv-sync:
	uv python install $(UV_PY)
	uv venv --python $(UV_PY)
	uv sync --extra test

uv-python:
	uv python install $(UV_PY)

test: uv-sync
	uv run pytest -q || pytest -q

uv-test:
	uv run pytest -q
