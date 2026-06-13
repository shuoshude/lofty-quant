from datetime import date
from pathlib import Path

import pandas as pd

from quant.config import load_config
from quant.etl import ETLTask
from quant.etl.fetch import (
    fetch_raw_data,
    find_raw_files,
    read_raw_csv,
    write_raw_csv,
)
from quant.utils import build_raw_path


def test_raw_csv_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "raw.csv"
    df = pd.DataFrame([{"cal_date": "20240102", "is_open": 1}])

    row_count = write_raw_csv(path, df)

    assert row_count == 1
    expected = pd.DataFrame([{"cal_date": "20240102", "is_open": "1"}])
    pd.testing.assert_frame_equal(read_raw_csv(path), expected)


def test_trade_calendar_raw_path_uses_single_file(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
    )
    path = build_raw_path(tmp_path, task)

    assert path == tmp_path / "tushare" / "trade-calendar" / "trade-calendar_tushare.csv"


def test_partitioned_raw_files_scan_multiple_months(tmp_path: Path) -> None:
    january_task = ETLTask(
        dataset="daily-ohlcv",
        source="akshare",
        start_date=date(2024, 1, 31),
        end_date=date(2024, 1, 31),
    )
    february_task = ETLTask(
        dataset="daily-ohlcv",
        source="akshare",
        start_date=date(2024, 2, 1),
        end_date=date(2024, 2, 1),
    )
    january_path = build_raw_path(tmp_path, january_task)
    february_path = build_raw_path(tmp_path, february_task)
    write_raw_csv(january_path, pd.DataFrame([{"trade_date": "20240131"}]))
    write_raw_csv(february_path, pd.DataFrame([{"trade_date": "20240201"}]))

    task = ETLTask(
        dataset="daily-ohlcv",
        source="akshare",
        start_date=date(2024, 1, 31),
        end_date=date(2024, 2, 1),
    )

    assert find_raw_files(tmp_path, task) == [january_path, february_path]


def test_fetch_raw_data_uses_config_raw_dir(monkeypatch, tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
    )

    def fake_fetch_tushare_raw(_config, _task):
        return pd.DataFrame([{"cal_date": "20240102", "is_open": 1}])

    monkeypatch.setattr("quant.etl.fetch._fetch_tushare_raw", fake_fetch_tushare_raw)

    path = fetch_raw_data(config, task)

    assert path == (
        config.paths.raw_dir / "tushare" / "trade-calendar" / "trade-calendar_tushare.csv"
    )
    assert path.exists()


def make_config_dir(tmp_path: Path) -> Path:
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
    return config_dir
