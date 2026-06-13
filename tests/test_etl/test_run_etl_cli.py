from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path
from types import ModuleType

from typer.testing import CliRunner

from quant.config import load_config
from quant.logger import setup_logger


def test_fetch_command_calls_fetch_function(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    calls: list[str] = []
    patch_runtime(monkeypatch, run_etl, tmp_path)

    def fake_fetch(raw_dir, task):
        calls.append(f"fetch:{raw_dir.name}:{task.dataset}:{task.source}")
        return raw_dir / "raw.jsonl"

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

    def fake_load(task):
        calls.append(f"load:{task.dataset}:{task.source}")
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
    assert calls == ["load:trade-calendar:tushare"]


def test_backfill_command_calls_fetch_then_load(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    calls: list[str] = []
    patch_runtime(monkeypatch, run_etl, tmp_path)

    def fake_fetch(raw_dir, task):
        calls.append("fetch")
        return raw_dir / "raw.jsonl"

    def fake_load(task):
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
            "tushare",
            "--start-date",
            "20240101",
            "--end-date",
            "20240131",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code != 0
    assert "暂未实现数据集: dataset=trade-calendar, source=tushare" in result.output


def test_status_command_reads_manifest(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    def fake_status(conn, *, dataset, source=None):
        return {
            "loaded_count": 1,
            "latest_trade_date": date(2024, 1, 2),
            "latest_loaded_at": None,
        }

    monkeypatch.setattr(run_etl, "get_manifest_status", fake_status)

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
    assert "加载记录数: 1" in result.output
    assert "最新交易日: 2024-01-02" in result.output


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
