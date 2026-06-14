from datetime import date
from pathlib import Path

import polars as pl

from quant.data.db import DuckDBManager


def test_initialize_empty_processed_dir_creates_physical_tables(tmp_path: Path) -> None:
    manager = DuckDBManager(tmp_path / "quant.duckdb", tmp_path / "processed")

    manager.initialize()

    with manager.session() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }

    assert {"dim_security", "dim_trade_calendar", "etl_manifest"}.issubset(tables)
    assert "v_daily_ohlcv" not in tables


def test_initialize_registers_parquet_views_and_adjusted_daily_view(tmp_path: Path) -> None:
    processed_dir = tmp_path / "processed"
    write_parquet(
        processed_dir / "ohlcv" / "year=2024" / "month=01" / "bars.parquet",
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [date(2024, 1, 2)],
            "open": [10.0],
            "high": [11.0],
            "low": [9.5],
            "close": [10.5],
            "pre_close": [10.0],
            "change": [0.5],
            "pct_chg": [5.0],
            "volume": [1000.0],
            "amount": [10500.0],
            "is_suspended": [False],
            "is_st": [False],
            "limit_status": ["none"],
        },
    )
    write_parquet(
        processed_dir / "ohlcv" / "year=2023" / "ohlcv_2023.parquet",
        {
            "ts_code": ["000002.SZ"],
            "trade_date": [date(2023, 12, 29)],
            "open": [20.0],
            "high": [21.0],
            "low": [19.5],
            "close": [20.5],
            "pre_close": [20.0],
            "change": [0.5],
            "pct_chg": [2.5],
            "volume": [2000.0],
            "amount": [41000.0],
            "is_suspended": [False],
            "is_st": [False],
            "limit_status": ["none"],
        },
    )
    write_parquet(
        processed_dir / "adj_factor" / "year=2024" / "month=01" / "adj.parquet",
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [date(2024, 1, 2)],
            "adj_factor": [2.0],
        },
    )
    write_parquet(
        processed_dir / "factors" / "year=2024" / "month=01" / "factors.parquet",
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [date(2024, 1, 2)],
            "factor_name": ["momentum_20d"],
            "factor_value": [1.23],
            "factor_version": ["v1"],
        },
    )
    manager = DuckDBManager(tmp_path / "quant.duckdb", processed_dir)

    manager.initialize()

    with manager.session() as conn:
        view_names = {
            row[0]
            for row in conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        adj_close = conn.execute(
            "SELECT adj_close FROM v_daily_adj WHERE ts_code = ?",
            ["000001.SZ"],
        ).fetchone()[0]
        daily_count = conn.execute("SELECT COUNT(*) FROM v_daily_ohlcv").fetchone()[0]

    assert {"v_daily_ohlcv", "v_adj_factor", "v_factors", "v_daily_adj"}.issubset(view_names)
    assert adj_close == 21.0
    assert daily_count == 2


def write_parquet(path: Path, data: dict[str, list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(data).write_parquet(path)
