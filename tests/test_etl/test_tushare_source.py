from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from quant.config import PathsConfig, ProjectConfig, QuantConfig, SecretsConfig
from quant.etl import ETLTask
from quant.etl.sources import tushare_source
from quant.etl.sources.tushare_source import TushareClient


def test_tushare_source_returns_dataframe(monkeypatch, tmp_path: Path) -> None:
    calls: dict[str, object] = {}

    class FakeApi:
        def trade_cal(self, *, exchange, start_date, end_date):
            calls["exchange"] = exchange
            calls["start_date"] = start_date
            calls["end_date"] = end_date
            return pd.DataFrame([{"exchange": "SSE", "cal_date": "20240102", "is_open": 1}])

    monkeypatch.setattr(tushare_source.ts, "set_token", lambda token: calls.update(token=token))
    monkeypatch.setattr(tushare_source.ts, "pro_api", lambda: FakeApi())

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
    assert list(df.columns) == ["exchange", "cal_date", "is_open"]


def test_tushare_source_requires_token(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="请在环境变量中设置 LOFTY_QUANT__SECRETS__TUSHARE_TOKEN"):
        TushareClient(make_config(tmp_path, token=None))


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
