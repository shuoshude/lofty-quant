from datetime import date
from pathlib import Path

from quant.etl.fetch import ETLTask, build_raw_path, find_raw_files, read_jsonl, write_jsonl


def test_raw_jsonl_roundtrip_and_discovery(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )
    path = build_raw_path(tmp_path, task)

    row_count = write_jsonl(path, [{"cal_date": "20240102", "is_open": 1}])

    assert row_count == 1
    assert path == (
        tmp_path
        / "tushare"
        / "trade-calendar"
        / "year=2024"
        / "month=01"
        / "trade-calendar_tushare_20240101_20240131.jsonl"
    )
    assert read_jsonl(path) == [{"cal_date": "20240102", "is_open": 1}]
    assert find_raw_files(tmp_path, task) == [path]


def test_find_raw_files_scans_multiple_months(tmp_path: Path) -> None:
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
    write_jsonl(january_path, [{"trade_date": "20240131"}])
    write_jsonl(february_path, [{"trade_date": "20240201"}])

    task = ETLTask(
        dataset="daily-ohlcv",
        source="akshare",
        start_date=date(2024, 1, 31),
        end_date=date(2024, 2, 1),
    )

    assert find_raw_files(tmp_path, task) == [january_path, february_path]
