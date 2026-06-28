ETL := uv run python scripts/run_etl.py
SOURCE ?= tushare
DATASET ?= daily-ohlcv
START_DATE ?=
END_DATE ?=
DATE ?=
YEAR ?=
FORCE ?= 0
DRY_RUN ?= 0
CONFIG_DIR ?=
ENVIRONMENT ?=
LOG_LEVEL ?= INFO
STRATEGY ?=

DATE_RANGE_FLAGS :=
ifneq ($(strip $(START_DATE)),)
DATE_RANGE_FLAGS += --start-date $(START_DATE)
endif
ifneq ($(strip $(END_DATE)),)
DATE_RANGE_FLAGS += --end-date $(END_DATE)
endif

RUN_DATE_FLAG :=
ifneq ($(strip $(DATE)),)
RUN_DATE_FLAG += --date $(DATE)
endif

FORCE_FLAG :=
ifeq ($(FORCE),1)
FORCE_FLAG += --force
endif

DRY_RUN_FLAG :=
ifeq ($(DRY_RUN),1)
DRY_RUN_FLAG += --dry-run
endif

CONFIG_FLAGS :=
ifneq ($(strip $(CONFIG_DIR)),)
CONFIG_FLAGS += --config-dir $(CONFIG_DIR)
endif
ifneq ($(strip $(ENVIRONMENT)),)
CONFIG_FLAGS += --environment $(ENVIRONMENT)
endif
CONFIG_FLAGS += --log-level $(LOG_LEVEL)

.PHONY: install etl etl-help etl-fetch etl-load etl-backfill etl-archive etl-status etl-missing etl-daily test test-fast lint format typecheck clean notebook init-db backtest

install:
	uv sync --all-extras

etl: etl-help

etl-help:
	$(ETL) --help

etl-fetch:
	$(ETL) fetch $(DATASET) --source $(SOURCE) $(DATE_RANGE_FLAGS) $(FORCE_FLAG) $(DRY_RUN_FLAG) $(CONFIG_FLAGS)

etl-load:
	$(ETL) load $(DATASET) --source $(SOURCE) $(DATE_RANGE_FLAGS) $(FORCE_FLAG) $(DRY_RUN_FLAG) $(CONFIG_FLAGS)

etl-backfill:
	$(ETL) backfill $(DATASET) --source $(SOURCE) $(DATE_RANGE_FLAGS) $(FORCE_FLAG) $(DRY_RUN_FLAG) $(CONFIG_FLAGS)

etl-archive:
	@test -n "$(YEAR)" || (echo "请传 YEAR=YYYY, 例如 make etl-archive DATASET=daily-ohlcv YEAR=2024"; exit 1)
	$(ETL) archive $(DATASET) --source $(SOURCE) --year $(YEAR) $(CONFIG_FLAGS)

etl-status:
	$(ETL) status $(DATASET) --source $(SOURCE) $(CONFIG_FLAGS)

etl-missing:
	$(ETL) missing $(DATASET) --source $(SOURCE) $(DATE_RANGE_FLAGS) $(CONFIG_FLAGS)

etl-daily:
	$(ETL) daily --source $(SOURCE) $(RUN_DATE_FLAG) $(FORCE_FLAG) $(DRY_RUN_FLAG) $(CONFIG_FLAGS)

backtest:
	uv run python scripts/run_backtest.py --strategy $(STRATEGY)

test:
	uv run pytest

test-fast:
	uv run pytest -x --no-cov

lint:
	uv run ruff check src/ tests/ scripts/ main.py

format:
	uv run ruff format .
	uv run ruff check --fix src/ tests/ scripts/ main.py

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
	uv run python -c "from quant.config import load_config; from quant.data.db import DuckDBManager; config = load_config(); DuckDBManager(config.paths.database_path, config.paths.processed_dir).initialize()"

