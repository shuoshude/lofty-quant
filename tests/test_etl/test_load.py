from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from quant.config import PathsConfig, ProjectConfig, QuantConfig, SecretsSettings
from quant.data.db import DuckDBManager
from quant.etl import ETLTask
from quant.etl.fetch import write_raw_csv
from quant.etl.load import load_raw_data
from quant.etl.sources.tushare_source import archive_daily_ohlcv_year
from quant.etl.storage import replace_duckdb_dataframe, replace_table_dataframe
from quant.utils import build_raw_path


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


def test_replace_table_dataframe_initializes_duckdb_and_replaces_rows(tmp_path: Path) -> None:
    database_path = tmp_path / "quant.duckdb"
    processed_dir = tmp_path / "processed"

    first_count = replace_table_dataframe(
        database_path,
        processed_dir,
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
    second_count = replace_table_dataframe(
        database_path,
        processed_dir,
        table="dim_security",
        df=pd.DataFrame(
            [
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "万科A",
                    "exchange": "SZSE",
                }
            ]
        ),
        columns=["ts_code", "symbol", "name", "exchange"],
        delete_where="exchange = ?",
        delete_params=["SZSE"],
    )

    manager = DuckDBManager(database_path, processed_dir)
    with manager.session() as conn:
        rows = conn.execute(
            "SELECT ts_code, name FROM dim_security ORDER BY ts_code"
        ).fetchall()

    assert first_count == 1
    assert second_count == 1
    assert rows == [("000002.SZ", "万科A")]


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


def test_load_daily_ohlcv_writes_monthly_processed_parquet(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = daily_task(date(2024, 1, 2), date(2024, 1, 2))
    write_daily_raw(config, date(2024, 1, 2), close="10.2")

    row_count = load_raw_data(config, task)

    output_path = processed_month_path(config, 2024, 1)
    df = pd.read_parquet(output_path)

    assert row_count == 1
    assert output_path.exists()
    assert df["ts_code"].tolist() == ["000001.SZ"]
    assert pd.to_datetime(df["trade_date"]).dt.date.tolist() == [date(2024, 1, 2)]
    assert df["volume"].tolist() == [1000.0]
    assert df["limit_status"].tolist() == ["none"]


def test_load_daily_ohlcv_writes_multiple_month_files(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = daily_task(date(2024, 1, 31), date(2024, 2, 1))
    write_daily_raw(config, date(2024, 1, 31), ts_code="000001.SZ")
    write_daily_raw(config, date(2024, 2, 1), ts_code="000002.SZ")

    row_count = load_raw_data(config, task)

    january_path = (
        config.paths.processed_dir / "ohlcv" / "year=2024" / "month=01" / "ohlcv_202401.parquet"
    )
    february_path = (
        config.paths.processed_dir / "ohlcv" / "year=2024" / "month=02" / "ohlcv_202402.parquet"
    )

    assert row_count == 2
    assert january_path.exists()
    assert february_path.exists()


def test_load_daily_ohlcv_overwrites_existing_key(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = daily_task(date(2024, 1, 2), date(2024, 1, 2))
    write_daily_raw(config, date(2024, 1, 2), close="10.2")
    load_raw_data(config, task)

    write_daily_raw(config, date(2024, 1, 2), close="11.2")
    load_raw_data(config, task)

    output_path = processed_month_path(config, 2024, 1)
    df = pd.read_parquet(output_path)

    assert len(df.index) == 1
    assert df["close"].tolist() == [11.2]


def test_load_daily_ohlcv_rejects_missing_raw(tmp_path: Path) -> None:
    config = make_config(tmp_path)

    with pytest.raises(FileNotFoundError, match="未找到日线行情 raw CSV 文件"):
        load_raw_data(config, daily_task(date(2024, 1, 2), date(2024, 1, 2)))


def test_load_daily_ohlcv_rejects_missing_column(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    raw_path = build_raw_path(config.paths.raw_dir, daily_task(date(2024, 1, 2), date(2024, 1, 2)))
    write_raw_csv(raw_path, pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20240102"}]))

    with pytest.raises(ValueError, match="日线行情 raw 缺少字段"):
        load_raw_data(config, daily_task(date(2024, 1, 2), date(2024, 1, 2)))


def test_load_daily_ohlcv_rejects_invalid_date_and_numeric(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    write_daily_raw(config, date(2024, 1, 2), trade_date="invalid")

    with pytest.raises(ValueError, match="日期字段 trade_date 格式无效"):
        load_raw_data(config, daily_task(date(2024, 1, 2), date(2024, 1, 2)))

    write_daily_raw(config, date(2024, 1, 2), trade_date="20240102", open_="bad")
    with pytest.raises(ValueError, match="数值字段 open 格式无效"):
        load_raw_data(config, daily_task(date(2024, 1, 2), date(2024, 1, 2)))


def test_load_daily_ohlcv_rejects_out_of_range_trade_date(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    write_daily_raw(config, date(2024, 1, 2), trade_date="20240103")

    with pytest.raises(ValueError, match="日线行情 raw 日期超出任务范围"):
        load_raw_data(config, daily_task(date(2024, 1, 2), date(2024, 1, 2)))


def test_load_daily_ohlcv_skips_empty_raw(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    raw_path = build_raw_path(config.paths.raw_dir, daily_task(date(2024, 1, 2), date(2024, 1, 2)))
    write_raw_csv(
        raw_path,
        pd.DataFrame(
            columns=[
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "vol",
                "amount",
            ]
        ),
    )

    row_count = load_raw_data(config, daily_task(date(2024, 1, 2), date(2024, 1, 2)))

    output_path = processed_month_path(config, 2024, 1)
    assert row_count == 0
    assert not output_path.exists()


def test_load_adj_factor_writes_monthly_processed_parquet(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = adj_factor_task(date(2024, 1, 2), date(2024, 1, 2))
    write_adj_factor_raw(config, date(2024, 1, 2), adj_factor="2.0")

    row_count = load_raw_data(config, task)

    output_path = adj_factor_month_path(config, 2024, 1)
    df = pd.read_parquet(output_path)

    assert row_count == 1
    assert output_path.exists()
    assert df["ts_code"].tolist() == ["000001.SZ"]
    assert pd.to_datetime(df["trade_date"]).dt.date.tolist() == [date(2024, 1, 2)]
    assert df["cumulative_factor"].tolist() == [2.0]


def test_load_adj_factor_overwrites_existing_key(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = adj_factor_task(date(2024, 1, 2), date(2024, 1, 2))
    write_adj_factor_raw(config, date(2024, 1, 2), adj_factor="2.0")
    load_raw_data(config, task)

    write_adj_factor_raw(config, date(2024, 1, 2), adj_factor="2.5")
    load_raw_data(config, task)

    df = pd.read_parquet(adj_factor_month_path(config, 2024, 1))

    assert len(df.index) == 1
    assert df["cumulative_factor"].tolist() == [2.5]


def test_load_adj_factor_rejects_missing_raw_and_invalid_rows(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = adj_factor_task(date(2024, 1, 2), date(2024, 1, 2))

    with pytest.raises(FileNotFoundError, match="未找到复权因子 raw CSV 文件"):
        load_raw_data(config, task)

    write_raw_csv(
        build_raw_path(config.paths.raw_dir, task),
        pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20240102"}]),
    )
    with pytest.raises(ValueError, match="复权因子 raw 缺少字段"):
        load_raw_data(config, task)

    write_adj_factor_raw(config, date(2024, 1, 2), adj_factor="0")
    with pytest.raises(ValueError, match=r"复权因子数据契约校验失败.*cumulative_factor"):
        load_raw_data(config, task)


def test_load_daily_basic_writes_monthly_processed_parquet(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = daily_basic_task(date(2024, 1, 2), date(2024, 1, 2))
    write_daily_basic_raw(config, date(2024, 1, 2), close="10.2")

    row_count = load_raw_data(config, task)

    output_path = daily_basic_month_path(config, 2024, 1)
    df = pd.read_parquet(output_path)

    assert row_count == 1
    assert output_path.exists()
    assert df["ts_code"].tolist() == ["000001.SZ"]
    assert pd.to_datetime(df["trade_date"]).dt.date.tolist() == [date(2024, 1, 2)]
    assert df["close"].tolist() == [10.2]
    assert df["turnover_rate_f"].tolist() == [2.5]
    assert df["dv_ttm"].tolist() == [0.6]


def test_load_daily_basic_writes_multiple_month_files(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = daily_basic_task(date(2024, 1, 31), date(2024, 2, 1))
    write_daily_basic_raw(config, date(2024, 1, 31), ts_code="000001.SZ")
    write_daily_basic_raw(config, date(2024, 2, 1), ts_code="000002.SZ")

    row_count = load_raw_data(config, task)

    assert row_count == 2
    assert daily_basic_month_path(config, 2024, 1).exists()
    assert daily_basic_month_path(config, 2024, 2).exists()


def test_load_daily_basic_overwrites_existing_key(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = daily_basic_task(date(2024, 1, 2), date(2024, 1, 2))
    write_daily_basic_raw(config, date(2024, 1, 2), close="10.2")
    load_raw_data(config, task)

    write_daily_basic_raw(config, date(2024, 1, 2), close="11.2")
    load_raw_data(config, task)

    df = pd.read_parquet(daily_basic_month_path(config, 2024, 1))

    assert len(df.index) == 1
    assert df["close"].tolist() == [11.2]


def test_load_daily_basic_normalizes_special_markers(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = daily_basic_task(date(2024, 1, 2), date(2024, 1, 2))
    write_daily_basic_raw(
        config,
        date(2024, 1, 2),
        volume_ratio="-1.0",
        pe="",
        pe_ttm="",
        dv_ratio="-1.0",
        dv_ttm="",
    )

    row_count = load_raw_data(config, task)

    df = pd.read_parquet(daily_basic_month_path(config, 2024, 1))

    assert row_count == 1
    assert df["volume_ratio"].tolist() == [0.0]
    assert df["pe"].tolist() == [-1.0]
    assert df["pe_ttm"].tolist() == [-1.0]
    assert df["dv_ratio"].tolist() == [0.0]
    assert df["dv_ttm"].tolist() == [0.0]


def test_load_daily_basic_rejects_missing_raw_and_invalid_rows(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = daily_basic_task(date(2024, 1, 2), date(2024, 1, 2))

    with pytest.raises(FileNotFoundError, match="未找到每日指标 raw CSV 文件"):
        load_raw_data(config, task)

    write_raw_csv(
        build_raw_path(config.paths.raw_dir, task),
        pd.DataFrame([{"ts_code": "000001.SZ"}]),
    )
    with pytest.raises(ValueError, match="每日指标 raw 缺少字段"):
        load_raw_data(config, task)

    write_daily_basic_raw(config, date(2024, 1, 2), trade_date="invalid")
    with pytest.raises(ValueError, match="日期字段 trade_date 格式无效"):
        load_raw_data(config, task)

    write_daily_basic_raw(config, date(2024, 1, 2), trade_date="20240103")
    with pytest.raises(ValueError, match="每日指标 raw 日期超出任务范围"):
        load_raw_data(config, task)

    write_daily_basic_raw(config, date(2024, 1, 2), close="bad")
    with pytest.raises(ValueError, match="数值字段 close 格式无效"):
        load_raw_data(config, task)

    write_daily_basic_raw(config, date(2024, 1, 2), turnover_rate="-1.0")
    with pytest.raises(ValueError, match=r"每日指标数据契约校验失败.*turnover_rate"):
        load_raw_data(config, task)


def test_load_daily_basic_skips_empty_raw(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    task = daily_basic_task(date(2024, 1, 2), date(2024, 1, 2))
    write_raw_csv(
        build_raw_path(config.paths.raw_dir, task),
        pd.DataFrame(columns=["ts_code", "trade_date"]),
    )

    row_count = load_raw_data(config, task)

    assert row_count == 0
    assert not daily_basic_month_path(config, 2024, 1).exists()


def test_archive_daily_ohlcv_year_merges_month_files_and_removes_them(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    archive_year = date.today().year - 1
    january_path = processed_month_path(config, archive_year, 1)
    february_path = processed_month_path(config, archive_year, 2)
    year_path = processed_year_path(config, archive_year)
    write_processed_daily(january_path, archive_year, 1, "000001.SZ", 10.0)
    write_processed_daily(february_path, archive_year, 2, "000002.SZ", 20.0)
    write_processed_daily(year_path, archive_year, 1, "000001.SZ", 8.0)

    output_path = archive_daily_ohlcv_year(config, archive_year)
    df = pd.read_parquet(output_path).sort_values("ts_code").reset_index(drop=True)

    assert output_path == year_path
    assert not january_path.exists()
    assert not february_path.exists()
    assert df["ts_code"].tolist() == ["000001.SZ", "000002.SZ"]
    assert df["close"].tolist() == [10.0, 20.0]


def test_archive_daily_ohlcv_year_rejects_current_year_and_missing_files(tmp_path: Path) -> None:
    config = make_config(tmp_path)

    with pytest.raises(ValueError, match="只能归档已结束年份"):
        archive_daily_ohlcv_year(config, date.today().year)

    with pytest.raises(FileNotFoundError, match="未找到可归档的月度日频文件"):
        archive_daily_ohlcv_year(config, date.today().year - 1)


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
        secrets=SecretsSettings(),
    )


def daily_task(start_date: date, end_date: date) -> ETLTask:
    return ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=start_date,
        end_date=end_date,
    )


def adj_factor_task(start_date: date, end_date: date) -> ETLTask:
    return ETLTask(
        dataset="adj-factor",
        source="tushare",
        start_date=start_date,
        end_date=end_date,
    )


def daily_basic_task(start_date: date, end_date: date) -> ETLTask:
    return ETLTask(
        dataset="daily-basic",
        source="tushare",
        start_date=start_date,
        end_date=end_date,
    )


def write_daily_raw(
    config: QuantConfig,
    raw_date: date,
    *,
    ts_code: str = "000001.SZ",
    trade_date: str | None = None,
    open_: str = "10.0",
    close: str = "10.2",
) -> None:
    task = daily_task(raw_date, raw_date)
    raw_path = build_raw_path(config.paths.raw_dir, task)
    write_raw_csv(
        raw_path,
        pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "trade_date": trade_date or raw_date.strftime("%Y%m%d"),
                    "open": open_,
                    "high": "12.0",
                    "low": "9.8",
                    "close": close,
                    "pre_close": "10.0",
                    "change": "0.2",
                    "pct_chg": "2.0",
                    "vol": "1000.0",
                    "amount": "10200.0",
                }
            ]
        ),
    )


def write_adj_factor_raw(
    config: QuantConfig,
    raw_date: date,
    *,
    ts_code: str = "000001.SZ",
    trade_date: str | None = None,
    adj_factor: str = "2.0",
) -> None:
    task = adj_factor_task(raw_date, raw_date)
    raw_path = build_raw_path(config.paths.raw_dir, task)
    write_raw_csv(
        raw_path,
        pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "trade_date": trade_date or raw_date.strftime("%Y%m%d"),
                    "adj_factor": adj_factor,
                }
            ]
        ),
    )


def write_daily_basic_raw(
    config: QuantConfig,
    raw_date: date,
    *,
    ts_code: str = "000001.SZ",
    trade_date: str | None = None,
    close: str = "10.2",
    turnover_rate: str = "1.5",
    volume_ratio: str = "1.2",
    pe: str = "10.0",
    pe_ttm: str = "11.0",
    dv_ratio: str = "0.5",
    dv_ttm: str = "0.6",
) -> None:
    task = daily_basic_task(raw_date, raw_date)
    raw_path = build_raw_path(config.paths.raw_dir, task)
    write_raw_csv(
        raw_path,
        pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "trade_date": trade_date or raw_date.strftime("%Y%m%d"),
                    "close": close,
                    "turnover_rate": turnover_rate,
                    "turnover_rate_f": "2.5",
                    "volume_ratio": volume_ratio,
                    "pe": pe,
                    "pe_ttm": pe_ttm,
                    "pb": "1.1",
                    "ps": "2.0",
                    "ps_ttm": "2.1",
                    "dv_ratio": dv_ratio,
                    "dv_ttm": dv_ttm,
                    "total_share": "100000.0",
                    "float_share": "80000.0",
                    "free_share": "60000.0",
                    "total_mv": "1000000.0",
                    "circ_mv": "800000.0",
                }
            ]
        ),
    )


def processed_month_path(config: QuantConfig, year: int, month: int) -> Path:
    return (
        config.paths.processed_dir
        / "ohlcv"
        / f"year={year}"
        / f"month={month:02d}"
        / f"ohlcv_{year}{month:02d}.parquet"
    )


def adj_factor_month_path(config: QuantConfig, year: int, month: int) -> Path:
    return (
        config.paths.processed_dir
        / "adj_factor"
        / f"year={year}"
        / f"month={month:02d}"
        / f"adj_factor_{year}{month:02d}.parquet"
    )


def daily_basic_month_path(config: QuantConfig, year: int, month: int) -> Path:
    return (
        config.paths.processed_dir
        / "daily_basic"
        / f"year={year}"
        / f"month={month:02d}"
        / f"daily_basic_{year}{month:02d}.parquet"
    )


def processed_year_path(config: QuantConfig, year: int) -> Path:
    return config.paths.processed_dir / "ohlcv" / f"year={year}" / f"ohlcv_{year}.parquet"


def write_processed_daily(path: Path, year: int, month: int, ts_code: str, close: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "trade_date": date(year, month, 2),
                "open": close,
                "high": close + 1,
                "low": close - 1,
                "close": close,
                "pre_close": close - 0.2,
                "change": 0.2,
                "pct_chg": 2.0,
                "volume": 1000.0,
                "amount": 10200.0,
                "is_suspended": False,
                "is_st": False,
                "limit_status": "none",
            }
        ]
    ).to_parquet(path, index=False)
