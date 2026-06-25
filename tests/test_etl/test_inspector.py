from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from quant.config import QuantConfig, load_config
from quant.data.db import DuckDBManager
from quant.etl.etl_model import ETLTask
from quant.etl.inspector import find_missing_dates, get_dataset_status
from quant.etl.raw import write_raw_csv
from quant.utils import build_raw_path


def test_get_dataset_status_reads_trade_calendar(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    insert_trade_calendar(config, [(date(2024, 1, 1), False), (date(2024, 1, 2), True)])

    state = get_dataset_status(config, "trade-calendar", source="tushare")

    assert state["exchange"] == "SSE"
    assert state["start_date"] == date(2024, 1, 1)
    assert state["end_date"] == date(2024, 1, 2)
    assert state["row_count"] == 2
    assert state["open_count"] == 1


def test_find_missing_dates_for_trade_calendar_uses_calendar_days(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    insert_trade_calendar(config, [(date(2024, 1, 2), True), (date(2024, 1, 4), True)])

    result = find_missing_dates(
        config,
        make_task("trade-calendar", date(2024, 1, 1), date(2024, 1, 4)),
    )

    assert result.expected_dates == (
        date(2024, 1, 1),
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
    )
    assert result.existing_dates == (date(2024, 1, 2), date(2024, 1, 4))
    assert result.missing_dates == (date(2024, 1, 1), date(2024, 1, 3))


@pytest.mark.parametrize(
    ("dataset", "processed_dir_name", "filename", "rows"),
    [
        (
            "daily-ohlcv",
            "ohlcv",
            "ohlcv_202401.parquet",
            [{"ts_code": "000001.SZ", "trade_date": date(2024, 1, 2), "close": 10.0}],
        ),
        (
            "adj-factor",
            "adj_factor",
            "adj_factor_202401.parquet",
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "cumulative_factor": 2.0,
                }
            ],
        ),
        (
            "daily-basic",
            "daily_basic",
            "daily_basic_202401.parquet",
            [{"ts_code": "000001.SZ", "trade_date": date(2024, 1, 2), "close": 10.0}],
        ),
    ],
)
def test_find_missing_dates_for_processed_daily_dataset(
    tmp_path: Path,
    dataset: str,
    processed_dir_name: str,
    filename: str,
    rows: list[dict[str, object]],
) -> None:
    config = make_config(tmp_path)
    insert_trade_calendar(
        config,
        [
            (date(2024, 1, 2), True),
            (date(2024, 1, 3), True),
            (date(2024, 1, 4), False),
        ],
    )
    write_processed_parquet(config, processed_dir_name, filename, rows)

    result = find_missing_dates(config, make_task(dataset, date(2024, 1, 2), date(2024, 1, 4)))

    assert result.expected_dates == (date(2024, 1, 2), date(2024, 1, 3))
    assert result.existing_dates == (date(2024, 1, 2),)
    assert result.missing_dates == (date(2024, 1, 3),)


def test_find_missing_dates_treats_missing_processed_view_as_empty(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    insert_trade_calendar(config, [(date(2024, 1, 2), True), (date(2024, 1, 3), True)])

    result = find_missing_dates(
        config,
        make_task("daily-ohlcv", date(2024, 1, 2), date(2024, 1, 3)),
    )

    assert result.existing_dates == ()
    assert result.missing_dates == (date(2024, 1, 2), date(2024, 1, 3))


def test_find_missing_dates_requires_full_trade_calendar_coverage(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    insert_trade_calendar(config, [(date(2026, 6, 1), True), (date(2026, 6, 2), True)])

    with pytest.raises(ValueError, match="交易日历未覆盖检查范围"):
        find_missing_dates(
            config,
            make_task("daily-ohlcv", date(2026, 6, 1), date(2026, 6, 5)),
        )


@pytest.mark.parametrize("dataset", ["stock-st", "stk-limit", "suspend-d"])
def test_find_missing_dates_for_raw_only_dataset(tmp_path: Path, dataset: str) -> None:
    config = make_config(tmp_path)
    insert_trade_calendar(
        config,
        [
            (date(2024, 1, 2), True),
            (date(2024, 1, 3), True),
            (date(2024, 1, 4), True),
        ],
    )
    task = make_task(dataset, date(2024, 1, 2), date(2024, 1, 4))
    for raw_date in (date(2024, 1, 2), date(2024, 1, 4)):
        raw_task = task.model_copy(update={"start_date": raw_date, "end_date": raw_date})
        write_raw_csv(
            build_raw_path(config.paths.raw_dir, raw_task),
            pd.DataFrame(columns=["ts_code", "trade_date"]),
        )

    result = find_missing_dates(config, task)

    assert result.expected_dates == (date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4))
    assert result.existing_dates == (date(2024, 1, 2), date(2024, 1, 4))
    assert result.missing_dates == (date(2024, 1, 3),)


def test_find_missing_dates_requires_full_trade_calendar_coverage_for_raw_only(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    insert_trade_calendar(config, [(date(2026, 6, 1), True), (date(2026, 6, 2), True)])

    with pytest.raises(ValueError, match="2026-06-03"):
        find_missing_dates(
            config,
            make_task("stock-st", date(2026, 6, 1), date(2026, 6, 3)),
        )


def test_find_missing_dates_requires_trade_calendar(tmp_path: Path) -> None:
    config = make_config(tmp_path)

    with pytest.raises(ValueError, match="交易日历未覆盖检查范围"):
        find_missing_dates(config, make_task("daily-ohlcv", date(2024, 1, 2), date(2024, 1, 3)))


def test_find_missing_dates_rejects_stock_basic_and_unknown_dataset(tmp_path: Path) -> None:
    config = make_config(tmp_path)

    with pytest.raises(NotImplementedError, match="stock-basic 是快照型数据集"):
        find_missing_dates(config, make_task("stock-basic", date(2024, 1, 2), date(2024, 1, 3)))

    with pytest.raises(NotImplementedError, match="暂未实现缺失日期检查"):
        find_missing_dates(config, make_task("unknown", date(2024, 1, 2), date(2024, 1, 3)))


def make_config(tmp_path: Path) -> QuantConfig:
    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "settings.toml").write_text(
        f"""
[project]
name = "test"

[paths]
raw_dir = "{(tmp_path / "raw").as_posix()}"
processed_dir = "{(tmp_path / "processed").as_posix()}"
database_path = "{(tmp_path / "db" / "quant.duckdb").as_posix()}"
notebooks_dir = "{(tmp_path / "notebooks").as_posix()}"
log_dir = "{(tmp_path / "log").as_posix()}"
""",
        encoding="utf-8",
    )
    return load_config(config_dir=config_dir)


def make_task(dataset: str, start_date: date, end_date: date) -> ETLTask:
    return ETLTask(
        dataset=dataset,
        source="tushare",
        start_date=start_date,
        end_date=end_date,
    )


def insert_trade_calendar(
    config: QuantConfig,
    calendar_rows: list[tuple[date, bool]],
) -> None:
    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
        conn.executemany(
            """
            INSERT INTO dim_trade_calendar (exchange, cal_date, is_open, pretrade_date)
            VALUES ('SSE', ?, ?, NULL)
            """,
            calendar_rows,
        )


def write_processed_parquet(
    config: QuantConfig,
    dataset_dir_name: str,
    filename: str,
    rows: list[dict[str, object]],
) -> None:
    path = config.paths.processed_dir / dataset_dir_name / "year=2024" / "month=01" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)
