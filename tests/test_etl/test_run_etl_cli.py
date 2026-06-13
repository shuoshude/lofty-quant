from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

from typer.testing import CliRunner

from quant.config import load_config
from quant.logger import setup_logger


def test_fetch_command_calls_fetch_function(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    calls: list[str] = []
    patch_runtime(monkeypatch, run_etl, tmp_path)

    def fake_fetch(config, task):
        calls.append(f"fetch:{config.paths.raw_dir.name}:{task.dataset}:{task.source}")
        return config.paths.raw_dir / "raw.csv"

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
        return config.paths.raw_dir / "raw.csv"

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
