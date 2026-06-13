.PHONY: install etl test lint format typecheck clean notebook

install:
	uv sync --all-extras

etl:
	uv run python scripts/run_etl.py --help

etl-status:
	uv run python scripts/run_etl.py status daily-ohlcv --source tushare

backtest:
	uv run python scripts/run_backtest.py --strategy $(STRATEGY)

test:
	uv run pytest

test-fast:
	uv run pytest -x --no-cov

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format .
	uv run ruff check --fix src/ tests/

typecheck:
	uv run mypy src/

notebook:
	uv run jupyter lab --no-browser

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	find . -name "*.pyc" -delete

init-db:
	uv run python -c "from quant.data.db import DuckDBManager; ..."



