from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from quant.config import PathsConfig, ProjectConfig, QuantConfig, SecretsConfig
from quant.data.db import DuckDBManager
from quant.etl import ETLTask
from quant.etl.fetch import write_raw_csv
from quant.etl.sources import tushare_source
from quant.etl.sources.tushare_source import (
    TushareClient,
    load_trade_calendar,
    normalize_trade_calendar_df,
)
from quant.utils import build_raw_path


def test_tushare_source_returns_dataframe(monkeypatch, tmp_path: Path) -> None:
    calls: dict[str, object] = {}
    sleep_calls: list[str] = []

    class FakeApi:
        def trade_cal(self, *, exchange, start_date, end_date):
            calls["exchange"] = exchange
            calls["start_date"] = start_date
            calls["end_date"] = end_date
            return pd.DataFrame([{"exchange": "SSE", "cal_date": "20240102", "is_open": 1}])

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: calls.update(token=token))
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(
        tushare_source,
        "_sleep_before_request",
        lambda: sleep_calls.append("sleep"),
    )

    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
    )

    df = TushareClient(make_config(tmp_path, token="test-token")).fetch_tushare_raw(task)

    assert calls == {
        "token": "test-token",
        "exchange": "SSE",
        "start_date": "20240101",
        "end_date": "20240131",
    }
    assert sleep_calls == ["sleep"]
    assert list(df.columns) == ["exchange", "cal_date", "is_open"]


def test_tushare_source_fetches_daily_ohlcv_from_open_trade_dates(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(
        config,
        [
            ("SSE", date(2024, 1, 2), True, date(2023, 12, 29)),
            ("SSE", date(2024, 1, 3), False, date(2024, 1, 2)),
            ("SSE", date(2024, 1, 4), True, date(2024, 1, 2)),
        ],
    )
    daily_calls: list[str] = []
    sleep_calls: list[str] = []

    class FakeApi:
        def daily(self, *, trade_date):
            daily_calls.append(trade_date)
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": trade_date,
                        "open": 10.0,
                        "high": 10.5,
                        "low": 9.8,
                        "close": 10.2,
                        "vol": 1000.0,
                        "amount": 10200.0,
                    }
                ]
            )

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(
        tushare_source,
        "_sleep_before_request",
        lambda: sleep_calls.append("sleep"),
    )

    task = ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 4),
        exchange="SSE",
    )

    frames_by_date = dict(TushareClient(config).fetch_daily_ohlcv(task))

    assert daily_calls == ["20240102", "20240104"]
    assert sleep_calls == ["sleep", "sleep"]
    assert list(frames_by_date) == [date(2024, 1, 2), date(2024, 1, 4)]
    assert frames_by_date[date(2024, 1, 2)]["trade_date"].tolist() == ["20240102"]
    assert frames_by_date[date(2024, 1, 4)]["trade_date"].tolist() == ["20240104"]


def test_tushare_source_daily_ohlcv_returns_empty_columns(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(config, [("SSE", date(2024, 1, 2), True, None)])

    class FakeApi:
        def daily(self, *, trade_date):
            return pd.DataFrame()

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(tushare_source, "_sleep_before_request", lambda: None)

    task = ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        exchange="SSE",
    )

    frames_by_date = dict(TushareClient(config).fetch_daily_ohlcv(task))

    df = frames_by_date[date(2024, 1, 2)]
    assert df.empty
    assert list(df.columns) == list(tushare_source.DAILY_OHLCV_RAW_COLUMNS)


def test_tushare_source_daily_ohlcv_requires_trade_calendar(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class FakeApi:
        def daily(self, *, trade_date):
            return pd.DataFrame()

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())

    task = ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        exchange="SSE",
    )

    with pytest.raises(ValueError, match="请先加载交易日历后再拉取日线行情"):
        list(TushareClient(make_config(tmp_path, token="test-token")).fetch_daily_ohlcv(task))


def test_tushare_source_requires_token(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="请在环境变量中设置 LOFTY_QUANT__SECRETS__TUSHARE_TOKEN"):
        TushareClient(make_config(tmp_path, token=None))


def test_normalize_trade_calendar_df_vectorizes_raw_dataframe() -> None:
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
    )
    raw_df = pd.DataFrame(
        [
            {"exchange": "", "cal_date": "20240102", "is_open": "1", "pretrade_date": ""},
            {
                "exchange": "szse",
                "cal_date": "20240103",
                "is_open": "0",
                "pretrade_date": "20240102",
            },
        ]
    )

    normalized = normalize_trade_calendar_df(raw_df, task)

    assert list(normalized.columns) == ["exchange", "cal_date", "is_open", "pretrade_date"]
    assert normalized.to_dict(orient="records") == [
        {
            "exchange": "SSE",
            "cal_date": date(2024, 1, 2),
            "is_open": True,
            "pretrade_date": None,
        },
        {
            "exchange": "SZSE",
            "cal_date": date(2024, 1, 3),
            "is_open": False,
            "pretrade_date": date(2024, 1, 2),
        },
    ]


def test_normalize_trade_calendar_df_rejects_invalid_date() -> None:
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    with pytest.raises(ValueError, match="日期字段 cal_date 格式无效"):
        normalize_trade_calendar_df(
            pd.DataFrame([{"cal_date": "invalid", "is_open": "1"}]),
            task,
        )


def test_load_trade_calendar_reads_single_raw_csv_and_writes_duckdb(tmp_path: Path) -> None:
    config = make_config(tmp_path, token=None)
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
                    "is_open": "1",
                    "pretrade_date": "20231229",
                }
            ]
        ),
    )

    row_count = load_trade_calendar(config, task)

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


def make_config(tmp_path: Path, *, token: str | None) -> QuantConfig:
    return QuantConfig(
        project=ProjectConfig(name="test"),
        paths=PathsConfig(
            raw_dir=tmp_path / "raw",
            processed_dir=tmp_path / "processed",
            database_path=tmp_path / "db" / "quant.duckdb",
            notebooks_dir=tmp_path / "notebooks",
            log_dir=tmp_path / "log",
        ),
        secrets=SecretsConfig(tushare_token=token),
    )


def write_trade_calendar(
    config: QuantConfig,
    rows: list[tuple[str, date, bool, date | None]],
) -> None:
    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
        conn.executemany(
            """
            INSERT INTO dim_trade_calendar (exchange, cal_date, is_open, pretrade_date)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )
