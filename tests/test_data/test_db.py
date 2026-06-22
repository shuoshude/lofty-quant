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
        calendar_comment = get_column_comment(conn, "dim_trade_calendar", "cal_date")
        security_comment = get_column_comment(conn, "dim_security", "ts_code")

    assert {"dim_security", "dim_trade_calendar", "etl_manifest"}.issubset(tables)
    assert "v_daily_ohlcv" not in tables
    assert calendar_comment == "自然日"
    assert security_comment == "证券代码, 使用 Tushare 交易所后缀格式"


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
            "cumulative_factor": [2.0],
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
        hfq_close = conn.execute(
            "SELECT hfq_close FROM v_daily_hfq WHERE ts_code = ?",
            ["000001.SZ"],
        ).fetchone()[0]
        qfq_close = conn.execute(
            "SELECT qfq_close FROM v_daily_qfq_latest WHERE ts_code = ?",
            ["000001.SZ"],
        ).fetchone()[0]
        daily_count = conn.execute("SELECT COUNT(*) FROM v_daily_ohlcv").fetchone()[0]
        daily_close_comment = get_column_comment(conn, "v_daily_ohlcv", "close")
        factor_comment = get_column_comment(conn, "v_adj_factor", "cumulative_factor")
        qfq_comment = get_column_comment(conn, "v_daily_qfq_latest", "qfq_close")

    assert {
        "v_daily_ohlcv",
        "v_adj_factor",
        "v_factors",
        "v_daily_hfq",
        "v_daily_qfq_latest",
        "v_daily_adj",
    }.issubset(view_names)
    assert hfq_close == 21.0
    assert qfq_close == 10.5
    assert daily_count == 2
    assert daily_close_comment == "收盘价"
    assert factor_comment == "累计复权因子"
    assert qfq_comment == "最新口径前复权收盘价"


def get_column_comment(conn, table_name: str, column_name: str) -> str | None:
    row = conn.execute(
        """
        SELECT comment
        FROM duckdb_columns()
        WHERE table_name = ?
          AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    return row[0] if row else None


def write_parquet(path: Path, data: dict[str, list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(data).write_parquet(path)
