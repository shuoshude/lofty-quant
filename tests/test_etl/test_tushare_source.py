from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from quant.config import PathsConfig, ProjectConfig, QuantConfig, SecretsSettings
from quant.data.db import DuckDBManager
from quant.data.fields import (
    TUSHARE_ADJ_FACTOR_RAW_COLUMNS,
    TUSHARE_DAILY_BASIC_RAW_COLUMNS,
    TUSHARE_DAILY_OHLCV_RAW_COLUMNS,
    TUSHARE_STK_LIMIT_RAW_COLUMNS,
    TUSHARE_STOCK_BASIC_RAW_COLUMNS,
    TUSHARE_STOCK_ST_RAW_COLUMNS,
    TUSHARE_SUSPEND_D_RAW_COLUMNS,
)
from quant.etl import ETLTask
from quant.etl.raw import write_raw_csv
from quant.etl.sources import tushare_normalizers, tushare_source
from quant.etl.sources.tushare_normalizers import (
    normalize_adj_factor_df,
    normalize_daily_basic_df,
    normalize_daily_ohlcv_df,
    normalize_trade_calendar_df,
)
from quant.etl.sources.tushare_source import TushareSource
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
        exchange="SZSE",
    )

    df = TushareSource(make_config(tmp_path, token="test-token")).fetch_raw(task)

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

    frames_by_date = dict(TushareSource(config).fetch_daily_ohlcv(task))

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

    frames_by_date = dict(TushareSource(config).fetch_daily_ohlcv(task))

    df = frames_by_date[date(2024, 1, 2)]
    assert df.empty
    assert list(df.columns) == list(TUSHARE_DAILY_OHLCV_RAW_COLUMNS)


def test_tushare_source_fetches_adj_factor_from_open_trade_dates(
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
    adj_factor_calls: list[str] = []
    sleep_calls: list[str] = []

    class FakeApi:
        def adj_factor(self, *, trade_date):
            adj_factor_calls.append(trade_date)
            return pd.DataFrame(
                [{"ts_code": "000001.SZ", "trade_date": trade_date, "adj_factor": 2.0}]
            )

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(
        tushare_source,
        "_sleep_before_request",
        lambda: sleep_calls.append("sleep"),
    )

    task = ETLTask(
        dataset="adj-factor",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 4),
        exchange="SSE",
    )

    frames_by_date = dict(TushareSource(config).fetch_adj_factor(task))

    assert adj_factor_calls == ["20240102", "20240104"]
    assert sleep_calls == ["sleep", "sleep"]
    assert list(frames_by_date) == [date(2024, 1, 2), date(2024, 1, 4)]
    assert frames_by_date[date(2024, 1, 2)]["adj_factor"].tolist() == [2.0]


def test_tushare_source_skips_existing_adj_factor_raw_before_request(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(config, [("SSE", date(2024, 1, 2), True, None)])
    task = ETLTask(
        dataset="adj-factor",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        exchange="SSE",
    )
    write_raw_csv(
        build_raw_path(config.paths.raw_dir, task),
        pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": "2.0"}]),
    )
    sleep_calls: list[str] = []

    class FakeApi:
        def adj_factor(self, *, trade_date):
            raise AssertionError("已有 raw 时不应调用 Tushare adj_factor 接口")

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(
        tushare_source,
        "_sleep_before_request",
        lambda: sleep_calls.append("sleep"),
    )

    frames_by_date = dict(TushareSource(config).fetch_adj_factor(task))

    assert sleep_calls == []
    assert list(frames_by_date) == [date(2024, 1, 2)]
    assert frames_by_date[date(2024, 1, 2)].empty


def test_tushare_source_adj_factor_returns_empty_columns(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(config, [("SSE", date(2024, 1, 2), True, None)])

    class FakeApi:
        def adj_factor(self, *, trade_date):
            return pd.DataFrame()

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(tushare_source, "_sleep_before_request", lambda: None)

    task = ETLTask(
        dataset="adj-factor",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        exchange="SSE",
    )

    frames_by_date = dict(TushareSource(config).fetch_adj_factor(task))

    df = frames_by_date[date(2024, 1, 2)]
    assert df.empty
    assert list(df.columns) == list(TUSHARE_ADJ_FACTOR_RAW_COLUMNS)


def test_tushare_source_fetches_daily_basic_from_open_trade_dates(
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
    daily_basic_calls: list[tuple[str, str]] = []
    sleep_calls: list[str] = []

    class FakeApi:
        def daily_basic(self, *, trade_date, fields):
            daily_basic_calls.append((trade_date, fields))
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": trade_date,
                        "close": 10.2,
                        "turnover_rate": 1.5,
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
        dataset="daily-basic",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 4),
        exchange="SSE",
    )

    frames_by_date = dict(TushareSource(config).fetch_daily_basic(task))

    expected_fields = ",".join(TUSHARE_DAILY_BASIC_RAW_COLUMNS)
    assert daily_basic_calls == [("20240102", expected_fields), ("20240104", expected_fields)]
    assert sleep_calls == ["sleep", "sleep"]
    assert list(frames_by_date) == [date(2024, 1, 2), date(2024, 1, 4)]
    assert frames_by_date[date(2024, 1, 2)]["turnover_rate"].tolist() == [1.5]


def test_tushare_source_skips_existing_daily_basic_raw_before_request(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(config, [("SSE", date(2024, 1, 2), True, None)])
    task = ETLTask(
        dataset="daily-basic",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        exchange="SSE",
    )
    write_raw_csv(
        build_raw_path(config.paths.raw_dir, task),
        pd.DataFrame(
            [{"ts_code": "000001.SZ", "trade_date": "20240102", "turnover_rate": "1.5"}]
        ),
    )
    sleep_calls: list[str] = []

    class FakeApi:
        def daily_basic(self, *, trade_date, fields):
            raise AssertionError("已有 raw 时不应调用 Tushare daily_basic 接口")

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(
        tushare_source,
        "_sleep_before_request",
        lambda: sleep_calls.append("sleep"),
    )

    frames_by_date = dict(TushareSource(config).fetch_daily_basic(task))

    assert sleep_calls == []
    assert list(frames_by_date) == [date(2024, 1, 2)]
    assert frames_by_date[date(2024, 1, 2)].empty


def test_tushare_source_daily_basic_returns_empty_columns(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(config, [("SSE", date(2024, 1, 2), True, None)])

    class FakeApi:
        def daily_basic(self, *, trade_date, fields):
            return pd.DataFrame()

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(tushare_source, "_sleep_before_request", lambda: None)

    task = ETLTask(
        dataset="daily-basic",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        exchange="SSE",
    )

    frames_by_date = dict(TushareSource(config).fetch_daily_basic(task))

    df = frames_by_date[date(2024, 1, 2)]
    assert df.empty
    assert list(df.columns) == list(TUSHARE_DAILY_BASIC_RAW_COLUMNS)


def test_tushare_source_fetches_stock_st_from_open_trade_dates(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(
        config,
        [
            ("SSE", date(2024, 1, 2), True, None),
            ("SSE", date(2024, 1, 3), False, date(2024, 1, 2)),
            ("SSE", date(2024, 1, 4), True, date(2024, 1, 2)),
        ],
    )
    stock_st_calls: list[tuple[str, str]] = []
    sleep_calls: list[str] = []

    class FakeApi:
        def stock_st(self, *, trade_date, fields):
            stock_st_calls.append((trade_date, fields))
            return pd.DataFrame(
                [{"ts_code": "000001.SZ", "name": "平安银行", "trade_date": trade_date}]
            )

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(
        tushare_source,
        "_sleep_before_request",
        lambda: sleep_calls.append("sleep"),
    )

    task = ETLTask(
        dataset="stock-st",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 4),
        exchange="SSE",
    )

    frames_by_date = dict(TushareSource(config).fetch_stock_st(task))

    expected_fields = ",".join(TUSHARE_STOCK_ST_RAW_COLUMNS)
    assert stock_st_calls == [("20240102", expected_fields), ("20240104", expected_fields)]
    assert sleep_calls == ["sleep", "sleep"]
    assert list(frames_by_date) == [date(2024, 1, 2), date(2024, 1, 4)]


def test_tushare_source_fetches_stk_limit_from_open_trade_dates(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(config, [("SSE", date(2024, 1, 2), True, None)])
    stk_limit_calls: list[tuple[str, str]] = []

    class FakeApi:
        def stk_limit(self, *, trade_date, fields):
            stk_limit_calls.append((trade_date, fields))
            return pd.DataFrame(
                [{"ts_code": "000001.SZ", "trade_date": trade_date, "up_limit": 11.0}]
            )

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(tushare_source, "_sleep_before_request", lambda: None)

    task = ETLTask(
        dataset="stk-limit",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        exchange="SSE",
    )

    frames_by_date = dict(TushareSource(config).fetch_stk_limit(task))

    assert stk_limit_calls == [("20240102", ",".join(TUSHARE_STK_LIMIT_RAW_COLUMNS))]
    assert frames_by_date[date(2024, 1, 2)]["up_limit"].tolist() == [11.0]


def test_tushare_source_fetches_suspend_d_from_open_trade_dates(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(config, [("SSE", date(2024, 1, 2), True, None)])
    suspend_d_calls: list[tuple[str, str]] = []

    class FakeApi:
        def suspend_d(self, *, trade_date, fields):
            suspend_d_calls.append((trade_date, fields))
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": trade_date,
                        "suspend_type": "S",
                    }
                ]
            )

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(tushare_source, "_sleep_before_request", lambda: None)

    task = ETLTask(
        dataset="suspend-d",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        exchange="SSE",
    )

    frames_by_date = dict(TushareSource(config).fetch_suspend_d(task))

    assert suspend_d_calls == [("20240102", ",".join(TUSHARE_SUSPEND_D_RAW_COLUMNS))]
    assert frames_by_date[date(2024, 1, 2)]["suspend_type"].tolist() == ["S"]


def test_tushare_source_raw_only_daily_datasets_return_empty_columns(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(config, [("SSE", date(2024, 1, 2), True, None)])

    class FakeApi:
        def stock_st(self, *, trade_date, fields):
            return pd.DataFrame()

        def stk_limit(self, *, trade_date, fields):
            return pd.DataFrame()

        def suspend_d(self, *, trade_date, fields):
            return pd.DataFrame()

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(tushare_source, "_sleep_before_request", lambda: None)

    source = TushareSource(config)
    task = ETLTask(
        dataset="stock-st",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        exchange="SSE",
    )

    assert list(dict(source.fetch_stock_st(task))[date(2024, 1, 2)].columns) == list(
        TUSHARE_STOCK_ST_RAW_COLUMNS
    )
    assert list(
        dict(source.fetch_stk_limit(task.model_copy(update={"dataset": "stk-limit"})))[
            date(2024, 1, 2)
        ].columns
    ) == list(TUSHARE_STK_LIMIT_RAW_COLUMNS)
    assert list(
        dict(source.fetch_suspend_d(task.model_copy(update={"dataset": "suspend-d"})))[
            date(2024, 1, 2)
        ].columns
    ) == list(TUSHARE_SUSPEND_D_RAW_COLUMNS)


def test_tushare_source_skips_existing_raw_only_daily_before_request(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    write_trade_calendar(config, [("SSE", date(2024, 1, 2), True, None)])
    task = ETLTask(
        dataset="stock-st",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        exchange="SSE",
    )
    write_raw_csv(
        build_raw_path(config.paths.raw_dir, task),
        pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20240102"}]),
    )

    class FakeApi:
        def stock_st(self, *, trade_date, fields):
            raise AssertionError("已有 raw 时不应调用 Tushare stock_st 接口")

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(tushare_source, "_sleep_before_request", lambda: None)

    frames_by_date = dict(TushareSource(config).fetch_stock_st(task))

    assert list(frames_by_date) == [date(2024, 1, 2)]
    assert frames_by_date[date(2024, 1, 2)].empty


def test_tushare_source_fetches_stock_basic_all_statuses(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")
    stock_basic_calls: list[tuple[str, str]] = []
    sleep_calls: list[str] = []

    class FakeApi:
        def stock_basic(self, *, list_status, fields):
            stock_basic_calls.append((list_status, fields))
            return pd.DataFrame(
                [
                    {
                        "ts_code": f"00000{len(stock_basic_calls)}.SZ",
                        "symbol": f"00000{len(stock_basic_calls)}",
                        "name": f"测试{list_status}",
                        "list_status": list_status,
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
        dataset="stock-basic",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
    )

    df = TushareSource(config).fetch_stock_basic(task)

    expected_fields = ",".join(TUSHARE_STOCK_BASIC_RAW_COLUMNS)
    assert stock_basic_calls == [
        ("L", expected_fields),
        ("D", expected_fields),
        ("P", expected_fields),
    ]
    assert sleep_calls == ["sleep", "sleep", "sleep"]
    assert df["list_status"].tolist() == ["L", "D", "P"]


def test_tushare_source_stock_basic_returns_empty_columns(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, token="test-token")

    class FakeApi:
        def stock_basic(self, *, list_status, fields):
            return pd.DataFrame()

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())
    monkeypatch.setattr(tushare_source, "_sleep_before_request", lambda: None)

    task = ETLTask(
        dataset="stock-basic",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
    )

    df = TushareSource(config).fetch_stock_basic(task)

    assert df.empty
    assert list(df.columns) == list(TUSHARE_STOCK_BASIC_RAW_COLUMNS)


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
        list(TushareSource(make_config(tmp_path, token="test-token")).fetch_daily_ohlcv(task))


def test_tushare_source_requires_token(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
    )

    with pytest.raises(ValueError, match="请在环境变量中设置 LOFTY_QUANT__SECRETS__TUSHARE_TOKEN"):
        TushareSource(make_config(tmp_path, token=None)).fetch_trade_calendar(task)


def test_tushare_source_load_raw_does_not_require_token(tmp_path: Path) -> None:
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
                    "is_open": 1,
                    "pretrade_date": "20231229",
                }
            ]
        ),
    )

    row_count = TushareSource(config).load_raw(task)

    assert row_count == 1


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


def test_normalize_daily_ohlcv_df_rejects_invalid_ts_code() -> None:
    raw_df = make_daily_raw_df(ts_code="000001")

    with pytest.raises(ValueError, match=r"日线行情数据契约校验失败.*ts_code"):
        normalize_daily_ohlcv_df(raw_df, make_daily_task())


def test_normalize_daily_ohlcv_df_rejects_invalid_price_range() -> None:
    high_below_low_df = make_daily_raw_df(high="8.0", low="9.0")
    open_outside_range_df = make_daily_raw_df(open_="12.0", high="11.0", low="9.0")

    with pytest.raises(ValueError, match="high 不能低于 low"):
        normalize_daily_ohlcv_df(high_below_low_df, make_daily_task())

    with pytest.raises(ValueError, match="open 必须位于 low 和 high 之间"):
        normalize_daily_ohlcv_df(open_outside_range_df, make_daily_task())


def test_normalize_daily_ohlcv_df_rejects_negative_volume_and_amount() -> None:
    negative_volume_df = make_daily_raw_df(vol="-1.0")
    negative_amount_df = make_daily_raw_df(amount="-1.0")

    with pytest.raises(ValueError, match="非停牌行成交量和成交额不能小于 0"):
        normalize_daily_ohlcv_df(negative_volume_df, make_daily_task())

    with pytest.raises(ValueError, match="非停牌行成交量和成交额不能小于 0"):
        normalize_daily_ohlcv_df(negative_amount_df, make_daily_task())


def test_normalize_daily_ohlcv_df_applies_stock_st_flags() -> None:
    normalized = normalize_daily_ohlcv_df(
        make_daily_raw_df(),
        make_daily_task(),
        stock_st_df=pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "trade_date": "20240102",
                    "type": "ST",
                    "type_name": "ST",
                }
            ]
        ),
    )

    assert normalized["is_st"].tolist() == [True]


def test_normalize_daily_ohlcv_df_calculates_limit_status() -> None:
    raw_df = pd.concat(
        [
            make_daily_raw_df(
                ts_code="000001.SZ",
                open_="10.0",
                high="12.0",
                low="8.0",
                close="10.0",
            ),
            make_daily_raw_df(
                ts_code="000002.SZ",
                open_="10.0",
                high="12.0",
                low="8.0",
                close="11.0",
            ),
            make_daily_raw_df(
                ts_code="000003.SZ",
                open_="10.0",
                high="12.0",
                low="8.0",
                close="12.0",
            ),
            make_daily_raw_df(
                ts_code="000004.SZ",
                open_="10.0",
                high="12.0",
                low="8.0",
                close="9.0",
            ),
            make_daily_raw_df(
                ts_code="000005.SZ",
                open_="10.0",
                high="12.0",
                low="8.0",
                close="8.0",
            ),
        ],
        ignore_index=True,
    )
    stk_limit_df = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "up_limit": 12.0, "down_limit": 8.0},
            {"ts_code": "000002.SZ", "trade_date": "20240102", "up_limit": 12.0, "down_limit": 8.0},
            {"ts_code": "000003.SZ", "trade_date": "20240102", "up_limit": 12.0, "down_limit": 8.0},
            {"ts_code": "000004.SZ", "trade_date": "20240102", "up_limit": 12.0, "down_limit": 8.0},
            {"ts_code": "000005.SZ", "trade_date": "20240102", "up_limit": 12.0, "down_limit": 8.0},
        ]
    )

    normalized = normalize_daily_ohlcv_df(raw_df, make_daily_task(), stk_limit_df=stk_limit_df)

    assert normalized.sort_values("ts_code")["limit_status"].tolist() == [0, 1, 2, 3, 4]


def test_normalize_daily_ohlcv_df_appends_full_day_suspension_rows() -> None:
    normalized = normalize_daily_ohlcv_df(
        make_daily_raw_df(ts_code="000001.SZ"),
        make_daily_task(),
        stock_st_df=pd.DataFrame(
            [
                {
                    "ts_code": "000002.SZ",
                    "name": "万科A",
                    "trade_date": "20240102",
                    "type": "ST",
                    "type_name": "ST",
                }
            ]
        ),
        suspend_d_df=pd.DataFrame(
            [
                {
                    "ts_code": "000002.SZ",
                    "trade_date": "20240102",
                    "suspend_timing": "",
                    "suspend_type": "S",
                },
                {
                    "ts_code": "000003.SZ",
                    "trade_date": "20240102",
                    "suspend_timing": "",
                    "suspend_type": "R",
                },
                {
                    "ts_code": "000004.SZ",
                    "trade_date": "20240102",
                    "suspend_timing": "09:30-10:30",
                    "suspend_type": "S",
                },
            ]
        ),
    ).sort_values("ts_code")

    assert normalized["ts_code"].tolist() == ["000001.SZ", "000002.SZ"]
    suspended_row = normalized[normalized["ts_code"] == "000002.SZ"].iloc[0]
    assert pd.isna(suspended_row["open"])
    assert bool(suspended_row["is_suspended"]) is True
    assert bool(suspended_row["is_st"]) is True
    assert suspended_row["limit_status"] == -1


def test_normalize_adj_factor_df_maps_to_cumulative_factor() -> None:
    normalized = normalize_adj_factor_df(make_adj_factor_raw_df(), make_adj_factor_task())

    assert list(normalized.columns) == ["ts_code", "trade_date", "cumulative_factor"]
    assert normalized.to_dict(orient="records") == [
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2024, 1, 2),
            "cumulative_factor": 2.0,
        }
    ]


def test_normalize_adj_factor_df_rejects_invalid_rows() -> None:
    with pytest.raises(ValueError, match="复权因子 raw 缺少字段"):
        normalize_adj_factor_df(pd.DataFrame([{"ts_code": "000001.SZ"}]), make_adj_factor_task())

    with pytest.raises(ValueError, match="日期字段 trade_date 格式无效"):
        normalize_adj_factor_df(
            make_adj_factor_raw_df(trade_date="invalid"),
            make_adj_factor_task(),
        )

    with pytest.raises(ValueError, match="复权因子 raw 日期超出任务范围"):
        normalize_adj_factor_df(
            make_adj_factor_raw_df(trade_date="20240103"),
            make_adj_factor_task(),
        )

    with pytest.raises(ValueError, match=r"复权因子数据契约校验失败.*cumulative_factor"):
        normalize_adj_factor_df(make_adj_factor_raw_df(adj_factor="0"), make_adj_factor_task())


def test_normalize_daily_basic_df_maps_official_fields() -> None:
    normalized = normalize_daily_basic_df(make_daily_basic_raw_df(), make_daily_basic_task())

    assert list(normalized.columns) == list(TUSHARE_DAILY_BASIC_RAW_COLUMNS)
    assert normalized.to_dict(orient="records") == [
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2024, 1, 2),
            "close": 10.2,
            "turnover_rate": 1.5,
            "turnover_rate_f": 2.5,
            "volume_ratio": 1.2,
            "pe": 10.0,
            "pe_ttm": 11.0,
            "pb": 1.1,
            "ps": 2.0,
            "ps_ttm": 2.1,
            "dv_ratio": 0.5,
            "dv_ttm": 0.6,
            "total_share": 100000.0,
            "float_share": 80000.0,
            "free_share": 60000.0,
            "total_mv": 1000000.0,
            "circ_mv": 800000.0,
        }
    ]


def test_normalize_daily_basic_df_normalizes_special_markers() -> None:
    normalized = normalize_daily_basic_df(
        make_daily_basic_raw_df(
            pe="",
            pe_ttm="nan",
            volume_ratio="-1",
            dv_ratio="-1",
            dv_ttm="",
        ),
        make_daily_basic_task(),
    )

    row = normalized.iloc[0].to_dict()
    assert row["pe"] == -1.0
    assert row["pe_ttm"] == -1.0
    assert row["volume_ratio"] == 0.0
    assert row["dv_ratio"] == 0.0
    assert row["dv_ttm"] == 0.0

    normalized_loss = normalize_daily_basic_df(
        make_daily_basic_raw_df(
            pe="-1",
            pe_ttm="-1",
            volume_ratio="",
            dv_ratio="0",
            dv_ttm="0",
        ),
        make_daily_basic_task(),
    )

    loss_row = normalized_loss.iloc[0].to_dict()
    assert loss_row["pe"] == -1.0
    assert loss_row["pe_ttm"] == -1.0
    assert loss_row["volume_ratio"] == 0.0
    assert loss_row["dv_ratio"] == 0.0
    assert loss_row["dv_ttm"] == 0.0


def test_normalize_daily_basic_df_appends_full_day_suspension_rows() -> None:
    normalized = normalize_daily_basic_df(
        make_daily_basic_raw_df(ts_code="000001.SZ"),
        make_daily_basic_task(),
        suspend_d_df=pd.DataFrame(
            [
                {
                    "ts_code": "000002.SZ",
                    "trade_date": "20240102",
                    "suspend_timing": "",
                    "suspend_type": "S",
                },
                {
                    "ts_code": "000003.SZ",
                    "trade_date": "20240102",
                    "suspend_timing": "",
                    "suspend_type": "R",
                },
                {
                    "ts_code": "000004.SZ",
                    "trade_date": "20240102",
                    "suspend_timing": "09:30-10:30",
                    "suspend_type": "S",
                },
            ]
        ),
        previous_records={
            "000002.SZ": {
                "ts_code": "000002.SZ",
                "trade_date": date(2024, 1, 1),
                "close": 8.8,
                "turnover_rate": 1.0,
                "turnover_rate_f": 1.5,
                "volume_ratio": 0.8,
                "pe": 15.0,
                "pe_ttm": 16.0,
                "pb": 1.2,
                "ps": 2.2,
                "ps_ttm": 2.3,
                "dv_ratio": 0.4,
                "dv_ttm": 0.5,
                "total_share": 200000.0,
                "float_share": 160000.0,
                "free_share": 120000.0,
                "total_mv": 2000000.0,
                "circ_mv": 1600000.0,
            }
        },
    ).sort_values("ts_code")

    assert normalized["ts_code"].tolist() == ["000001.SZ", "000002.SZ"]
    suspended_row = normalized[normalized["ts_code"] == "000002.SZ"].iloc[0]
    assert pd.isna(suspended_row["close"])
    assert pd.isna(suspended_row["turnover_rate"])
    assert pd.isna(suspended_row["turnover_rate_f"])
    assert pd.isna(suspended_row["volume_ratio"])
    assert suspended_row["pe"] == 15.0
    assert suspended_row["pe_ttm"] == 16.0
    assert suspended_row["total_share"] == 200000.0


def test_normalize_daily_basic_df_rejects_missing_suspended_previous_record() -> None:
    with pytest.raises(ValueError, match="无法补全每日指标停牌行"):
        normalize_daily_basic_df(
            make_daily_basic_raw_df(ts_code="000001.SZ"),
            make_daily_basic_task(),
            suspend_d_df=pd.DataFrame(
                [
                    {
                        "ts_code": "000002.SZ",
                        "trade_date": "20240102",
                        "suspend_timing": "",
                        "suspend_type": "S",
                    }
                ]
            ),
            previous_records={},
        )


def test_normalize_daily_basic_df_logs_and_zeroes_anomaly_fields(monkeypatch) -> None:
    error_logs: list[tuple[str, tuple[object, ...]]] = []

    class FakeLogger:
        def bind(self, **_kwargs):
            return self

        def error(self, message, *args):
            error_logs.append((message, args))

    monkeypatch.setattr(tushare_normalizers, "logger", FakeLogger())

    normalized = normalize_daily_basic_df(
        make_daily_basic_raw_df(
            turnover_rate="-1",
            turnover_rate_f="0",
            total_share="",
            free_share="-1",
            float_share="0",
            total_mv="-10",
            circ_mv="0",
        ),
        make_daily_basic_task(),
    )

    row = normalized.iloc[0].to_dict()
    assert row["turnover_rate"] == 0.0
    assert row["turnover_rate_f"] == 0.0
    assert row["total_share"] == 0.0
    assert row["free_share"] == 0.0
    assert row["float_share"] == 0.0
    assert row["total_mv"] == 0.0
    assert row["circ_mv"] == 0.0
    assert len(error_logs) == 7
    assert all("每日指标 raw 存在异常指标字段" in message for message, _args in error_logs)


def test_normalize_daily_basic_df_rejects_invalid_rows() -> None:
    with pytest.raises(ValueError, match="每日指标 raw 缺少字段"):
        normalize_daily_basic_df(pd.DataFrame([{"ts_code": "000001.SZ"}]), make_daily_basic_task())

    with pytest.raises(ValueError, match="日期字段 trade_date 格式无效"):
        normalize_daily_basic_df(
            make_daily_basic_raw_df(trade_date="invalid"),
            make_daily_basic_task(),
        )

    with pytest.raises(ValueError, match="每日指标 raw 日期超出任务范围"):
        normalize_daily_basic_df(
            make_daily_basic_raw_df(trade_date="20240103"),
            make_daily_basic_task(),
        )

    with pytest.raises(ValueError, match="数值字段 close 格式无效"):
        normalize_daily_basic_df(make_daily_basic_raw_df(close="bad"), make_daily_basic_task())


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

    row_count = TushareSource(config).load_trade_calendar(task)

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


def test_load_stock_basic_reads_single_raw_csv_and_replaces_duckdb(tmp_path: Path) -> None:
    config = make_config(tmp_path, token=None)
    task = ETLTask(
        dataset="stock-basic",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
    )
    raw_path = build_raw_path(config.paths.raw_dir, task)
    write_raw_csv(raw_path, make_stock_basic_raw_df(ts_code="000001.SZ", name="平安银行"))

    row_count = TushareSource(config).load_stock_basic(task)
    write_raw_csv(raw_path, make_stock_basic_raw_df(ts_code="000002.SZ", name="万科A"))
    second_count = TushareSource(config).load_stock_basic(task)

    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    with manager.session() as conn:
        rows = conn.execute(
            """
            SELECT ts_code, name, list_status, list_date
            FROM dim_security
            ORDER BY ts_code
            """
        ).fetchall()

    assert row_count == 1
    assert second_count == 1
    assert rows == [("000002.SZ", "万科A", "L", "19910129")]


def test_load_stock_basic_rejects_missing_raw_and_missing_columns(tmp_path: Path) -> None:
    config = make_config(tmp_path, token=None)
    task = ETLTask(
        dataset="stock-basic",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
    )

    with pytest.raises(FileNotFoundError, match="未找到股票基础信息 raw CSV 文件"):
        TushareSource(config).load_stock_basic(task)

    write_raw_csv(
        build_raw_path(config.paths.raw_dir, task),
        pd.DataFrame([{"ts_code": "000001.SZ", "symbol": "000001"}]),
    )
    with pytest.raises(ValueError, match="股票基础信息 raw 缺少字段"):
        TushareSource(config).load_stock_basic(task)


def make_daily_task() -> ETLTask:
    return ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
    )


def make_adj_factor_task() -> ETLTask:
    return ETLTask(
        dataset="adj-factor",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
    )


def make_daily_basic_task() -> ETLTask:
    return ETLTask(
        dataset="daily-basic",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
    )


def make_daily_raw_df(
    *,
    ts_code: str = "000001.SZ",
    trade_date: str = "20240102",
    open_: str = "10.0",
    high: str = "11.0",
    low: str = "9.0",
    close: str = "10.5",
    vol: str = "1000.0",
    amount: str = "10500.0",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "trade_date": trade_date,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "pre_close": "10.0",
                "change": "0.5",
                "pct_chg": "5.0",
                "vol": vol,
                "amount": amount,
            }
        ]
    )


def make_adj_factor_raw_df(
    *,
    ts_code: str = "000001.SZ",
    trade_date: str = "20240102",
    adj_factor: str = "2.0",
) -> pd.DataFrame:
    return pd.DataFrame(
        [{"ts_code": ts_code, "trade_date": trade_date, "adj_factor": adj_factor}]
    )


def make_daily_basic_raw_df(
    *,
    ts_code: str = "000001.SZ",
    trade_date: str = "20240102",
    close: str = "10.2",
    turnover_rate: str = "1.5",
    turnover_rate_f: str = "2.5",
    volume_ratio: str = "1.2",
    pe: str = "10.0",
    pe_ttm: str = "11.0",
    dv_ratio: str = "0.5",
    dv_ttm: str = "0.6",
    total_share: str = "100000.0",
    float_share: str = "80000.0",
    free_share: str = "60000.0",
    total_mv: str = "1000000.0",
    circ_mv: str = "800000.0",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "trade_date": trade_date,
                "close": close,
                "turnover_rate": turnover_rate,
                "turnover_rate_f": turnover_rate_f,
                "volume_ratio": volume_ratio,
                "pe": pe,
                "pe_ttm": pe_ttm,
                "pb": "1.1",
                "ps": "2.0",
                "ps_ttm": "2.1",
                "dv_ratio": dv_ratio,
                "dv_ttm": dv_ttm,
                "total_share": total_share,
                "float_share": float_share,
                "free_share": free_share,
                "total_mv": total_mv,
                "circ_mv": circ_mv,
            }
        ]
    )


def make_stock_basic_raw_df(
    *,
    ts_code: str = "000001.SZ",
    symbol: str = "000001",
    name: str = "平安银行",
    list_status: str = "L",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "symbol": symbol,
                "name": name,
                "area": "深圳",
                "industry": "银行",
                "fullname": f"{name}股份有限公司",
                "enname": "Test Co., Ltd.",
                "cnspell": "cs",
                "market": "主板",
                "exchange": "SZSE",
                "curr_type": "CNY",
                "list_status": list_status,
                "list_date": "19910129",
                "delist_date": "",
                "is_hs": "S",
                "act_name": "",
                "act_ent_type": "",
            }
        ]
    )


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
        secrets=SecretsSettings(tushare_token=token),
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
