from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest
from pydantic import BaseModel

from quant.config import PathsConfig, ProjectConfig, QuantConfig, SecretsConfig
from quant.data.db import DuckDBManager
from quant.etl import ETLTask
from quant.etl.fetch import write_raw_csv
from quant.etl.load import load_raw_data
from quant.etl.storage import (
    get_manifest_status,
    insert_duckdb_records,
    replace_duckdb_dataframe,
    replace_duckdb_records,
    write_manifest,
    write_processed_parquet,
)
from quant.utils import build_raw_path


class SecurityRecord(BaseModel):
    ts_code: str
    symbol: str
    name: str
    exchange: str


def test_write_processed_parquet_partitions_by_year_and_month(tmp_path: Path) -> None:
    output_path = write_processed_parquet(
        tmp_path,
        dataset="ohlcv",
        partition_date=date(2024, 1, 2),
        records=[{"ts_code": "000001.SZ", "trade_date": date(2024, 1, 2)}],
    )

    assert output_path == tmp_path / "ohlcv" / "year=2024" / "month=01" / "ohlcv_20240102.parquet"
    assert output_path.exists()


def test_insert_and_replace_duckdb_records(tmp_path: Path) -> None:
    manager = DuckDBManager(tmp_path / "quant.duckdb", tmp_path / "processed")
    manager.initialize()

    with manager.session() as conn:
        inserted = insert_duckdb_records(
            conn,
            table="dim_security",
            records=[
                SecurityRecord(
                    ts_code="000001.SZ",
                    symbol="000001",
                    name="平安银行",
                    exchange="SZSE",
                )
            ],
            columns=["ts_code", "symbol", "name", "exchange"],
        )
        replaced = replace_duckdb_records(
            conn,
            table="dim_security",
            records=[
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行A",
                    "exchange": "SZSE",
                }
            ],
            columns=["ts_code", "symbol", "name", "exchange"],
            delete_where="ts_code = ?",
            delete_params=["000001.SZ"],
        )
        name = conn.execute(
            "SELECT name FROM dim_security WHERE ts_code = ?",
            ["000001.SZ"],
        ).fetchone()[0]

    assert inserted == 1
    assert replaced == 1
    assert name == "平安银行A"


def test_replace_duckdb_dataframe(tmp_path: Path) -> None:
    manager = DuckDBManager(tmp_path / "quant.duckdb", tmp_path / "processed")
    manager.initialize()

    with manager.session() as conn:
        row_count = replace_duckdb_dataframe(
            conn,
            table="dim_security",
            df=pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "symbol": "000001",
                        "name": "平安银行",
                        "exchange": "SZSE",
                    }
                ]
            ),
            columns=["ts_code", "symbol", "name", "exchange"],
            delete_where="exchange = ?",
            delete_params=["SZSE"],
        )
        name = conn.execute(
            "SELECT name FROM dim_security WHERE ts_code = ?",
            ["000001.SZ"],
        ).fetchone()[0]

    assert row_count == 1
    assert name == "平安银行"


def test_replace_duckdb_dataframe_rejects_invalid_identifier(tmp_path: Path) -> None:
    manager = DuckDBManager(tmp_path / "quant.duckdb", tmp_path / "processed")
    manager.initialize()

    with (
        manager.session() as conn,
        pytest.raises(ValueError, match="无效的 SQL 标识符"),
    ):
        replace_duckdb_dataframe(
            conn,
            table="dim_security; DROP TABLE dim_security",
            df=pd.DataFrame(),
            columns=["ts_code"],
            delete_where="ts_code = ?",
            delete_params=["000001.SZ"],
        )


def test_write_manifest_and_status(tmp_path: Path) -> None:
    manager = DuckDBManager(tmp_path / "quant.duckdb", tmp_path / "processed")
    manager.initialize()

    with manager.session() as conn:
        write_manifest(
            conn,
            dataset="daily-ohlcv",
            trade_date=date(2024, 1, 2),
            source="tushare",
            version="default",
            row_count=1,
            loaded_at=datetime(2024, 1, 2, 18, 0, 0),
        )
        write_manifest(
            conn,
            dataset="daily-ohlcv",
            trade_date=date(2024, 1, 2),
            source="tushare",
            version="default",
            row_count=2,
            loaded_at=datetime(2024, 1, 2, 19, 0, 0),
        )
        status = get_manifest_status(conn, dataset="daily-ohlcv", source="tushare")

    assert status["loaded_count"] == 1
    assert status["latest_trade_date"] == date(2024, 1, 2)
    assert status["latest_loaded_at"] == datetime(2024, 1, 2, 19, 0, 0)


def test_load_trade_calendar_from_raw_csv(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
    )
    raw_path = build_raw_path(config.paths.raw_dir, task)
    write_raw_csv(
        raw_path,
        pd.DataFrame(
            [
                {
                    "exchange": "SSE",
                    "cal_date": "20240102",
                    "is_open": 1,
                    "pretrade_date": "20231229",
                }
            ]
        ),
    )

    row_count = load_raw_data(config, task)

    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    with manager.session() as conn:
        calendar_row = conn.execute(
            """
            SELECT exchange, cal_date, is_open, pretrade_date
            FROM dim_trade_calendar
            WHERE exchange = ? AND cal_date = ?
            """,
            ["SSE", date(2024, 1, 2)],
        ).fetchone()

    assert row_count == 1
    assert calendar_row == ("SSE", date(2024, 1, 2), True, date(2023, 12, 29))


def test_load_raw_data_rejects_unknown_source(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = ETLTask(
        dataset="trade-calendar",
        source="unknown",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    with pytest.raises(NotImplementedError, match="暂未实现数据源: source=unknown"):
        load_raw_data(config, task)


def test_load_daily_ohlcv_is_placeholder(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
    )

    with pytest.raises(NotImplementedError, match="暂未实现日线行情 load"):
        load_raw_data(config, task)


def make_config(tmp_path: Path) -> QuantConfig:
    return QuantConfig(
        project=ProjectConfig(name="test"),
        paths=PathsConfig(
            raw_dir=tmp_path / "raw",
            processed_dir=tmp_path / "processed",
            database_path=tmp_path / "db" / "quant.duckdb",
            notebooks_dir=tmp_path / "notebooks",
            log_dir=tmp_path / "log",
        ),
        secrets=SecretsConfig(),
    )
