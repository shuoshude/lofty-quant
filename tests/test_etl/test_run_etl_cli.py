from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path
from types import ModuleType

import pandas as pd
from typer.testing import CliRunner

from quant.config import load_config
from quant.logger import setup_logger


def test_fetch_command_calls_fetch_function(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    calls: list[str] = []
    patch_runtime(monkeypatch, run_etl, tmp_path)

    def fake_fetch(config, task):
        calls.append(f"fetch:{config.paths.raw_dir.name}:{task.dataset}:{task.source}")
        return (config.paths.raw_dir / "raw.csv",)

    monkeypatch.setattr(run_etl, "fetch_raw_data", fake_fetch)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "fetch",
            "trade-calendar",
            "--source",
            "tushare",
            "--start-date",
            "20240101",
            "--end-date",
            "20240131",
        ],
    )

    assert result.exit_code == 0
    assert calls == ["fetch:raw:trade-calendar:tushare"]
    assert "原始数据落盘完成:" in result.output


def test_fetch_command_can_print_multiple_raw_paths(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    patch_runtime(monkeypatch, run_etl, tmp_path)

    def fake_fetch(config, task):
        return (
            config.paths.raw_dir / "daily-ohlcv_tushare_20240102.csv",
            config.paths.raw_dir / "daily-ohlcv_tushare_20240103.csv",
        )

    monkeypatch.setattr(run_etl, "fetch_raw_data", fake_fetch)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "fetch",
            "daily-ohlcv",
            "--source",
            "tushare",
            "--start-date",
            "20240102",
            "--end-date",
            "20240103",
        ],
    )

    assert result.exit_code == 0
    assert "daily-ohlcv_tushare_20240102.csv" in result.output
    assert "daily-ohlcv_tushare_20240103.csv" in result.output


def test_load_command_calls_load_function(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    calls: list[str] = []
    patch_runtime(monkeypatch, run_etl, tmp_path)

    def fake_load(config, task):
        calls.append(f"load:{config.paths.raw_dir.name}:{task.dataset}:{task.source}")
        return 3

    monkeypatch.setattr(run_etl, "load_raw_data", fake_load)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "load",
            "trade-calendar",
            "--source",
            "tushare",
            "--start-date",
            "20240101",
            "--end-date",
            "20240131",
        ],
    )

    assert result.exit_code == 0
    assert calls == ["load:raw:trade-calendar:tushare"]


def test_backfill_command_calls_fetch_then_load(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    calls: list[str] = []
    patch_runtime(monkeypatch, run_etl, tmp_path)

    def fake_fetch(config, task):
        calls.append("fetch")
        return (config.paths.raw_dir / "raw.csv",)

    def fake_load(config, task):
        calls.append("load")
        return 2

    monkeypatch.setattr(run_etl, "fetch_raw_data", fake_fetch)
    monkeypatch.setattr(run_etl, "load_raw_data", fake_load)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "backfill",
            "trade-calendar",
            "--source",
            "tushare",
            "--start-date",
            "20240101",
            "--end-date",
            "20240131",
        ],
    )

    assert result.exit_code == 0
    assert calls == ["fetch", "load"]


def test_load_daily_ohlcv_reports_missing_raw_file(tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "load",
            "daily-ohlcv",
            "--source",
            "tushare",
            "--start-date",
            "20240102",
            "--end-date",
            "20240102",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code != 0
    assert "未找到日线行情 raw CSV 文件" in result.output


def test_archive_command_calls_archive_function(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    patch_runtime(monkeypatch, run_etl, tmp_path)
    calls: list[str] = []

    from quant.etl.sources import tushare_source

    def fake_archive(config, year):
        calls.append(f"archive:{config.paths.processed_dir.name}:{year}")
        return config.paths.processed_dir / "ohlcv" / f"year={year}" / f"ohlcv_{year}.parquet"

    monkeypatch.setattr(tushare_source, "archive_daily_ohlcv_year", fake_archive)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "archive",
            "daily-ohlcv",
            "--source",
            "tushare",
            "--year",
            "2025",
        ],
    )

    assert result.exit_code == 0
    assert calls == ["archive:processed:2025"]
    assert "归档完成:" in result.output


def test_unimplemented_dataset_returns_chinese_error(tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "fetch",
            "trade-calendar",
            "--source",
            "unknown",
            "--start-date",
            "20240101",
            "--end-date",
            "20240131",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code != 0
    assert "暂未实现数据集: dataset=trade-calendar, source=unknown" in result.output


def test_status_command_reads_trade_calendar_table(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    def fake_status(conn):
        return {
            "exchange": "SSE",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "row_count": 31,
            "open_count": 22,
        }

    monkeypatch.setattr(run_etl, "_get_trade_calendar_status", fake_status)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "status",
            "trade-calendar",
            "--source",
            "tushare",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code == 0
    assert "交易所: SSE" in result.output
    assert "起始日期: 2024-01-01" in result.output
    assert "结束日期: 2024-01-31" in result.output
    assert "日历行数: 31" in result.output
    assert "开市天数: 22" in result.output


def test_status_command_reads_daily_ohlcv_processed_state(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    def fake_status(config):
        return {
            "start_date": "2024-01-02",
            "end_date": "2024-01-31",
            "row_count": 100,
            "trade_date_count": 22,
            "security_count": 5,
        }

    monkeypatch.setattr(run_etl, "_get_daily_ohlcv_status", fake_status)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "status",
            "daily-ohlcv",
            "--source",
            "tushare",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code == 0
    assert "数据集: daily-ohlcv" in result.output
    assert "行情行数: 100" in result.output
    assert "交易日数: 22" in result.output
    assert "证券数: 5" in result.output


def test_daily_ohlcv_status_reads_processed_view(tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config = load_config(config_dir=make_config_dir(tmp_path))
    parquet_path = (
        config.paths.processed_dir
        / "ohlcv"
        / "year=2024"
        / "month=01"
        / "ohlcv_202401.parquet"
    )
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 2), "close": 10.0},
            {"ts_code": "000002.SZ", "trade_date": date(2024, 1, 2), "close": 20.0},
            {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 3), "close": 10.5},
        ]
    ).to_parquet(parquet_path, index=False)

    state = run_etl._get_daily_ohlcv_status(config)

    assert state["start_date"] == date(2024, 1, 2)
    assert state["end_date"] == date(2024, 1, 3)
    assert state["row_count"] == 3
    assert state["trade_date_count"] == 2
    assert state["security_count"] == 2


def patch_runtime(monkeypatch, run_etl: ModuleType, tmp_path: Path) -> None:
    config_dir = make_config_dir(tmp_path)

    def setup_from_test_config(*_args):
        config = load_config(config_dir=config_dir)
        setup_logger(config=config, enable_console=False)
        return config

    monkeypatch.setattr(run_etl, "_setup_runtime", setup_from_test_config)


def make_config_dir(tmp_path: Path) -> Path:
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
    return config_dir


def load_run_etl_module() -> ModuleType:
    script_path = Path(__file__).parents[2] / "scripts" / "run_etl.py"
    spec = importlib.util.spec_from_file_location("run_etl_script", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载脚本: {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
