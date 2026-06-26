from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from quant.config import QuantConfig, load_config
from quant.etl.daily_pipeline import run_daily_pipeline
from quant.etl.inspector import MissingDateResult


def test_daily_pipeline_runs_open_day_steps_in_order(monkeypatch, tmp_path: Path) -> None:
    from quant.etl import daily_pipeline

    config = make_config(tmp_path)
    calls: list[str] = []

    def fake_fetch(_config, task):
        calls.append(f"fetch:{task.dataset}:{task.force}:{task.dry_run}")
        return (_config.paths.raw_dir / f"{task.dataset}.csv",)

    def fake_load(_config, task):
        calls.append(f"load:{task.dataset}:{task.force}:{task.dry_run}")
        return 1

    def fake_missing(_config, task):
        calls.append(f"missing:{task.dataset}:{task.force}:{task.dry_run}")
        return MissingDateResult(
            dataset=task.dataset,
            source=task.source,
            start_date=task.start_date,
            end_date=task.end_date,
            expected_dates=(task.start_date,),
            existing_dates=(task.start_date,),
            missing_dates=(),
        )

    monkeypatch.setattr(daily_pipeline, "fetch_raw_data", fake_fetch)
    monkeypatch.setattr(daily_pipeline, "load_raw_data", fake_load)
    monkeypatch.setattr(daily_pipeline, "find_missing_dates", fake_missing)
    monkeypatch.setattr(daily_pipeline, "_load_trade_date_open_state", lambda *_args: True)

    result = run_daily_pipeline(
        config,
        "tushare",
        date(2026, 6, 25),
        force=True,
        dry_run=True,
    )

    assert result.is_open is True
    assert len(result.steps) == 19
    assert calls == [
        "fetch:trade-calendar:True:True",
        "load:trade-calendar:True:True",
        "fetch:stock-basic:True:True",
        "load:stock-basic:True:True",
        "fetch:daily-ohlcv:True:True",
        "fetch:stock-st:True:True",
        "fetch:stk-limit:True:True",
        "fetch:suspend-d:True:True",
        "load:daily-ohlcv:True:True",
        "fetch:adj-factor:True:True",
        "load:adj-factor:True:True",
        "fetch:daily-basic:True:True",
        "load:daily-basic:True:True",
        "missing:daily-ohlcv:True:False",
        "missing:adj-factor:True:False",
        "missing:daily-basic:True:False",
        "missing:stock-st:True:False",
        "missing:stk-limit:True:False",
        "missing:suspend-d:True:False",
    ]


def test_daily_pipeline_skips_daily_steps_on_closed_day(monkeypatch, tmp_path: Path) -> None:
    from quant.etl import daily_pipeline

    config = make_config(tmp_path)
    calls: list[str] = []

    def fake_fetch(_config, task):
        calls.append(f"fetch:{task.dataset}")
        return (_config.paths.raw_dir / f"{task.dataset}.csv",)

    def fake_load(_config, task):
        calls.append(f"load:{task.dataset}")
        return 1

    monkeypatch.setattr(daily_pipeline, "fetch_raw_data", fake_fetch)
    monkeypatch.setattr(daily_pipeline, "load_raw_data", fake_load)
    monkeypatch.setattr(daily_pipeline, "_load_trade_date_open_state", lambda *_args: False)

    result = run_daily_pipeline(config, "tushare", date(2026, 6, 20))

    assert result.is_open is False
    assert len(result.steps) == 4
    assert calls == [
        "fetch:trade-calendar",
        "load:trade-calendar",
        "fetch:stock-basic",
        "load:stock-basic",
    ]


def test_daily_pipeline_stops_after_failed_step_and_logs_exception(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from quant.etl import daily_pipeline

    config = make_config(tmp_path)
    calls: list[str] = []
    logger_events: list[str] = []

    class FakeLogger:
        def bind(self, **_kwargs):
            return self

        def info(self, *_args, **_kwargs):
            logger_events.append("info")

        def exception(self, *_args, **_kwargs):
            logger_events.append("exception")

    def fake_fetch(_config, task):
        calls.append(f"fetch:{task.dataset}")
        return (_config.paths.raw_dir / f"{task.dataset}.csv",)

    def fake_load(_config, task):
        calls.append(f"load:{task.dataset}")
        if task.dataset == "daily-ohlcv":
            raise ValueError("加载失败")
        return 1

    monkeypatch.setattr(daily_pipeline, "logger", FakeLogger())
    monkeypatch.setattr(daily_pipeline, "fetch_raw_data", fake_fetch)
    monkeypatch.setattr(daily_pipeline, "load_raw_data", fake_load)
    monkeypatch.setattr(daily_pipeline, "_load_trade_date_open_state", lambda *_args: True)

    with pytest.raises(ValueError, match="加载失败"):
        run_daily_pipeline(config, "tushare", date(2026, 6, 25))

    assert calls == [
        "fetch:trade-calendar",
        "load:trade-calendar",
        "fetch:stock-basic",
        "load:stock-basic",
        "fetch:daily-ohlcv",
        "fetch:stock-st",
        "fetch:stk-limit",
        "fetch:suspend-d",
        "load:daily-ohlcv",
    ]
    assert "exception" in logger_events


def test_daily_pipeline_rejects_unsupported_source(tmp_path: Path) -> None:
    config = make_config(tmp_path)

    with pytest.raises(NotImplementedError, match="暂未实现每日管线"):
        run_daily_pipeline(config, "unknown", date(2026, 6, 25))


def make_config(tmp_path: Path) -> QuantConfig:
    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "settings.toml").write_text(
        f"""
[project]
name = "test"

[paths]
raw_dir = "{(tmp_path / "raw").as_posix()}"
processed_dir = "{(tmp_path / "processed").as_posix()}"
database_path = "{(tmp_path / "db" / "quant.duckdb").as_posix()}"
notebooks_dir = "{(tmp_path / "notebooks").as_posix()}"
log_dir = "{(tmp_path / "log").as_posix()}"
""",
        encoding="utf-8",
    )
    return load_config(config_dir=config_dir)
