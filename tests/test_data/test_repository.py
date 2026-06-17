from datetime import date
from pathlib import Path

import polars as pl

from quant.data.db import DuckDBManager
from quant.data.repository import QuantRepository


def test_repository_returns_daily_bars_ordered_by_date(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        rows = repository.get_daily_bars(
            "000001.SZ",
            date(2024, 1, 1),
            date(2024, 1, 3),
            adjusted=True,
        )

    assert [row["trade_date"] for row in rows] == [date(2024, 1, 2), date(2024, 1, 3)]
    assert [row["adj_close"] for row in rows] == [21.0, 24.0]


def test_repository_cross_section_can_exclude_suspended_rows(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        rows = repository.get_cross_section(
            date(2024, 1, 2),
            ["ts_code", "close", "is_suspended"],
            exclude_suspended=True,
        )

    assert rows == [{"ts_code": "000001.SZ", "close": 10.5, "is_suspended": False}]


def test_repository_filters_factors_by_name_and_version(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        rows = repository.get_factors(
            date(2024, 1, 2),
            ["momentum_20d", "size"],
            factor_version="v1",
        )

    assert rows == [
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2024, 1, 2),
            "factor_name": "momentum_20d",
            "factor_value": 1.23,
            "factor_version": "v1",
        }
    ]


def test_repository_returns_trade_calendar(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        conn.execute(
            """
            INSERT INTO dim_trade_calendar (exchange, cal_date, is_open, pretrade_date)
            VALUES
                ('SSE', DATE '2024-01-01', FALSE, DATE '2023-12-29'),
                ('SSE', DATE '2024-01-02', TRUE, DATE '2023-12-29')
            """
        )
        repository = QuantRepository(conn)
        rows = repository.get_trade_calendar(date(2024, 1, 1), date(2024, 1, 2))

    assert [row["is_open"] for row in rows] == [False, True]


def test_repository_returns_open_trade_dates_filtered_and_ordered(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        conn.execute(
            """
            INSERT INTO dim_trade_calendar (exchange, cal_date, is_open, pretrade_date)
            VALUES
                ('SSE', DATE '2024-01-04', TRUE, DATE '2024-01-02'),
                ('SSE', DATE '2024-01-02', TRUE, DATE '2023-12-29'),
                ('SSE', DATE '2024-01-03', FALSE, DATE '2024-01-02'),
                ('SZSE', DATE '2024-01-02', TRUE, DATE '2023-12-29'),
                ('SSE', DATE '2024-01-05', TRUE, DATE '2024-01-04')
            """
        )
        repository = QuantRepository(conn)
        trade_dates = repository.get_open_trade_dates(
            date(2024, 1, 2),
            date(2024, 1, 4),
            exchange="SSE",
        )

    assert trade_dates == [date(2024, 1, 2), date(2024, 1, 4)]


def initialized_manager(tmp_path: Path) -> DuckDBManager:
    processed_dir = tmp_path / "processed"
    write_parquet(
        processed_dir / "ohlcv" / "year=2024" / "month=01" / "bars.parquet",
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000002.SZ"],
            "trade_date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 2)],
            "open": [10.0, 11.0, 5.0],
            "high": [11.0, 12.0, 5.5],
            "low": [9.5, 10.5, 4.8],
            "close": [10.5, 12.0, 5.1],
            "pre_close": [10.0, 10.5, 5.0],
            "change": [0.5, 1.5, 0.1],
            "pct_chg": [5.0, 14.29, 2.0],
            "volume": [1000.0, 1500.0, 2000.0],
            "amount": [10500.0, 18000.0, 10200.0],
            "is_suspended": [False, False, True],
            "is_st": [False, False, False],
            "limit_status": ["none", "up", "none"],
        },
    )
    write_parquet(
        processed_dir / "adj_factor" / "year=2024" / "month=01" / "adj.parquet",
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000002.SZ"],
            "trade_date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 2)],
            "adj_factor": [2.0, 2.0, 1.0],
        },
    )
    write_parquet(
        processed_dir / "factors" / "year=2024" / "month=01" / "factors.parquet",
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": [date(2024, 1, 2), date(2024, 1, 2)],
            "factor_name": ["momentum_20d", "momentum_20d"],
            "factor_value": [1.23, 9.99],
            "factor_version": ["v1", "v2"],
        },
    )
    manager = DuckDBManager(tmp_path / "quant.duckdb", processed_dir)
    manager.initialize()
    return manager


def write_parquet(path: Path, data: dict[str, list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(data).write_parquet(path)
