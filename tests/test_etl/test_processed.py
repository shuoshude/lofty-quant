from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from quant.etl.processed import (
    archive_daily_year,
    build_daily_month_path,
    load_daily_raw_csv_to_monthly_parquet,
    write_daily_month_parquet,
    write_parquet_atomic,
)

DAILY_COLUMNS = (
    "ts_code",
    "trade_date",
    "close",
    "volume",
)
DAILY_KEYS = ("ts_code", "trade_date")


def test_write_daily_month_parquet_writes_single_month_file(tmp_path: Path) -> None:
    df = make_daily_df([("000001.SZ", date(2024, 1, 2), 10.0)])

    written = write_daily_month_parquet(
        tmp_path,
        "ohlcv",
        df,
        date_column="trade_date",
        key_columns=DAILY_KEYS,
        columns=DAILY_COLUMNS,
    )

    output_path = build_daily_month_path(tmp_path, "ohlcv", 2024, 1)
    output_df = pd.read_parquet(output_path)

    assert written == {output_path: 1}
    assert output_df["ts_code"].tolist() == ["000001.SZ"]
    assert output_df["close"].tolist() == [10.0]


def test_write_daily_month_parquet_splits_cross_month_data(tmp_path: Path) -> None:
    df = make_daily_df(
        [
            ("000001.SZ", date(2024, 1, 31), 10.0),
            ("000002.SZ", date(2024, 2, 1), 20.0),
        ]
    )

    written = write_daily_month_parquet(
        tmp_path,
        "ohlcv",
        df,
        date_column="trade_date",
        key_columns=DAILY_KEYS,
        columns=DAILY_COLUMNS,
    )

    assert set(written) == {
        build_daily_month_path(tmp_path, "ohlcv", 2024, 1),
        build_daily_month_path(tmp_path, "ohlcv", 2024, 2),
    }


def test_write_daily_month_parquet_overwrites_existing_key(tmp_path: Path) -> None:
    old_df = make_daily_df([("000001.SZ", date(2024, 1, 2), 8.0)])
    new_df = make_daily_df([("000001.SZ", date(2024, 1, 2), 10.0)])

    write_daily_month_parquet(
        tmp_path,
        "ohlcv",
        old_df,
        date_column="trade_date",
        key_columns=DAILY_KEYS,
        columns=DAILY_COLUMNS,
    )
    write_daily_month_parquet(
        tmp_path,
        "ohlcv",
        new_df,
        date_column="trade_date",
        key_columns=DAILY_KEYS,
        columns=DAILY_COLUMNS,
    )

    output_df = pd.read_parquet(build_daily_month_path(tmp_path, "ohlcv", 2024, 1))
    assert len(output_df.index) == 1
    assert output_df["close"].tolist() == [10.0]


def test_write_daily_month_parquet_skips_empty_dataframe(tmp_path: Path) -> None:
    df = pd.DataFrame(columns=DAILY_COLUMNS)

    written = write_daily_month_parquet(
        tmp_path,
        "ohlcv",
        df,
        date_column="trade_date",
        key_columns=DAILY_KEYS,
        columns=DAILY_COLUMNS,
    )

    assert written == {}
    assert not (tmp_path / "ohlcv").exists()


def test_write_parquet_atomic_replaces_file_and_cleans_tmp(tmp_path: Path) -> None:
    output_path = tmp_path / "ohlcv.parquet"
    write_parquet_atomic(output_path, pd.DataFrame([{"ts_code": "000001.SZ", "close": 8.0}]))

    write_parquet_atomic(output_path, pd.DataFrame([{"ts_code": "000001.SZ", "close": 10.0}]))

    output_df = pd.read_parquet(output_path)
    assert output_df["close"].tolist() == [10.0]
    assert not (tmp_path / ".ohlcv.tmp.parquet").exists()


def test_load_daily_raw_csv_to_monthly_parquet_writes_multiple_raw_files(tmp_path: Path) -> None:
    january_raw = write_raw_csv(tmp_path / "raw" / "20240131.csv", "000001.SZ", "2024-01-31", 10.0)
    february_raw = write_raw_csv(tmp_path / "raw" / "20240201.csv", "000002.SZ", "2024-02-01", 20.0)

    result = load_daily_raw_csv_to_monthly_parquet(
        [january_raw, february_raw],
        tmp_path / "processed",
        "ohlcv",
        read_frame=pd.read_csv,
        normalize_frame=normalize_raw_daily_df,
        date_column="trade_date",
        key_columns=DAILY_KEYS,
        columns=DAILY_COLUMNS,
    )

    january_path = build_daily_month_path(tmp_path / "processed", "ohlcv", 2024, 1)
    february_path = build_daily_month_path(tmp_path / "processed", "ohlcv", 2024, 2)
    assert result.row_count == 2
    assert set(result.written_paths) == {january_path, february_path}
    assert pd.read_parquet(january_path)["ts_code"].tolist() == ["000001.SZ"]
    assert pd.read_parquet(february_path)["ts_code"].tolist() == ["000002.SZ"]


def test_load_daily_raw_csv_to_monthly_parquet_skips_empty_normalized_df(
    tmp_path: Path,
) -> None:
    raw_path = write_raw_csv(tmp_path / "raw" / "empty.csv", "000001.SZ", "2024-01-02", 10.0)

    result = load_daily_raw_csv_to_monthly_parquet(
        [raw_path],
        tmp_path / "processed",
        "ohlcv",
        read_frame=pd.read_csv,
        normalize_frame=lambda _df: pd.DataFrame(columns=DAILY_COLUMNS),
        date_column="trade_date",
        key_columns=DAILY_KEYS,
        columns=DAILY_COLUMNS,
    )

    assert result.row_count == 0
    assert result.written_paths == {}
    assert not (tmp_path / "processed" / "ohlcv").exists()


def test_load_daily_raw_csv_to_monthly_parquet_dry_run_does_not_write(
    tmp_path: Path,
) -> None:
    raw_path = write_raw_csv(tmp_path / "raw" / "20240102.csv", "000001.SZ", "2024-01-02", 10.0)

    result = load_daily_raw_csv_to_monthly_parquet(
        [raw_path],
        tmp_path / "processed",
        "ohlcv",
        read_frame=pd.read_csv,
        normalize_frame=normalize_raw_daily_df,
        date_column="trade_date",
        key_columns=DAILY_KEYS,
        columns=DAILY_COLUMNS,
        dry_run=True,
    )

    assert result.row_count == 1
    assert result.written_paths == {}
    assert not (tmp_path / "processed" / "ohlcv").exists()


def test_archive_daily_year_merges_month_files_and_removes_them(tmp_path: Path) -> None:
    archive_year = date.today().year - 1
    january_path = build_daily_month_path(tmp_path, "ohlcv", archive_year, 1)
    february_path = build_daily_month_path(tmp_path, "ohlcv", archive_year, 2)
    year_path = tmp_path / "ohlcv" / f"year={archive_year}" / f"ohlcv_{archive_year}.parquet"
    write_parquet(january_path, make_daily_df([("000001.SZ", date(archive_year, 1, 2), 10.0)]))
    write_parquet(february_path, make_daily_df([("000002.SZ", date(archive_year, 2, 2), 20.0)]))
    write_parquet(year_path, make_daily_df([("000001.SZ", date(archive_year, 1, 2), 8.0)]))

    output_path = archive_daily_year(
        tmp_path,
        "ohlcv",
        archive_year,
        key_columns=DAILY_KEYS,
        columns=DAILY_COLUMNS,
    )
    output_df = pd.read_parquet(output_path).sort_values("ts_code").reset_index(drop=True)

    assert output_path == year_path
    assert not january_path.exists()
    assert not february_path.exists()
    assert not january_path.parent.exists()
    assert not february_path.parent.exists()
    assert output_df["ts_code"].tolist() == ["000001.SZ", "000002.SZ"]
    assert output_df["close"].tolist() == [10.0, 20.0]


def test_archive_daily_year_keeps_non_empty_month_directory(tmp_path: Path) -> None:
    archive_year = date.today().year - 1
    january_path = build_daily_month_path(tmp_path, "ohlcv", archive_year, 1)
    keep_path = january_path.parent / "README.txt"
    write_parquet(january_path, make_daily_df([("000001.SZ", date(archive_year, 1, 2), 10.0)]))
    keep_path.write_text("保留非归档文件", encoding="utf-8")

    archive_daily_year(
        tmp_path,
        "ohlcv",
        archive_year,
        key_columns=DAILY_KEYS,
        columns=DAILY_COLUMNS,
    )

    assert not january_path.exists()
    assert january_path.parent.exists()
    assert keep_path.exists()


def test_archive_daily_year_rejects_current_year_and_missing_month_files(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="只能归档已结束年份"):
        archive_daily_year(
            tmp_path,
            "ohlcv",
            date.today().year,
            key_columns=DAILY_KEYS,
            columns=DAILY_COLUMNS,
        )

    with pytest.raises(FileNotFoundError, match="未找到可归档的月度日频文件"):
        archive_daily_year(
            tmp_path,
            "ohlcv",
            date.today().year - 1,
            key_columns=DAILY_KEYS,
            columns=DAILY_COLUMNS,
        )


def make_daily_df(rows: list[tuple[str, date, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "trade_date": trade_date,
                "close": close,
                "volume": 1000.0,
            }
            for ts_code, trade_date, close in rows
        ]
    )


def write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def write_raw_csv(path: Path, ts_code: str, trade_date: str, close: float) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [{"code": ts_code, "date": trade_date, "close": close, "volume": 1000.0}]
    ).to_csv(path, index=False)
    return path


def normalize_raw_daily_df(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": df["code"],
            "trade_date": pd.to_datetime(df["date"]).dt.date,
            "close": df["close"],
            "volume": df["volume"],
        }
    )
