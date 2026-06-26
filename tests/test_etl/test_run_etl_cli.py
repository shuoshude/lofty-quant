from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path
from types import ModuleType

import pandas as pd
from typer.testing import CliRunner

from quant.config import load_config
from quant.data.db import DuckDBManager
from quant.etl.daily_pipeline import DailyPipelineResult, DailyPipelineStepResult
from quant.etl.inspector import MissingDateResult, get_dataset_status
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


def test_fetch_command_supports_raw_only_daily_datasets(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    calls: list[str] = []
    patch_runtime(monkeypatch, run_etl, tmp_path)

    def fake_fetch(config, task):
        calls.append(task.dataset)
        return (
            config.paths.raw_dir / task.dataset / f"{task.dataset}_tushare_20240102.csv",
        )

    monkeypatch.setattr(run_etl, "fetch_raw_data", fake_fetch)

    for dataset in ("stock-st", "stk-limit", "suspend-d"):
        result = CliRunner().invoke(
            run_etl.app,
            [
                "fetch",
                dataset,
                "--source",
                "tushare",
                "--start-date",
                "20240102",
                "--end-date",
                "20240102",
            ],
        )

        assert result.exit_code == 0
        assert f"{dataset}_tushare_20240102.csv" in result.output

    assert calls == ["stock-st", "stk-limit", "suspend-d"]


def test_stock_basic_commands_allow_omitted_dates(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    calls: list[str] = []
    patch_runtime(monkeypatch, run_etl, tmp_path)

    def fake_fetch(config, task):
        calls.append(f"fetch:{task.dataset}:{task.start_date}:{task.end_date}")
        return (config.paths.raw_dir / "stock-basic_tushare.csv",)

    def fake_load(_config, task):
        calls.append(f"load:{task.dataset}:{task.start_date}:{task.end_date}")
        return 3

    monkeypatch.setattr(run_etl, "fetch_raw_data", fake_fetch)
    monkeypatch.setattr(run_etl, "load_raw_data", fake_load)

    fetch_result = CliRunner().invoke(
        run_etl.app,
        ["fetch", "stock-basic", "--source", "tushare"],
    )
    load_result = CliRunner().invoke(
        run_etl.app,
        ["load", "stock-basic", "--source", "tushare"],
    )
    backfill_result = CliRunner().invoke(
        run_etl.app,
        ["backfill", "stock-basic", "--source", "tushare"],
    )

    assert fetch_result.exit_code == 0
    assert load_result.exit_code == 0
    assert backfill_result.exit_code == 0
    assert [call.split(":")[:2] for call in calls] == [
        ["fetch", "stock-basic"],
        ["load", "stock-basic"],
        ["fetch", "stock-basic"],
        ["load", "stock-basic"],
    ]


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


def test_raw_only_daily_datasets_do_not_support_load_or_backfill(
    monkeypatch,
    tmp_path: Path,
) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    load_result = CliRunner().invoke(
        run_etl.app,
        [
            "load",
            "stock-st",
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

    def fake_fetch(config, task):
        return (config.paths.raw_dir / "stock-st_tushare_20240102.csv",)

    monkeypatch.setattr(run_etl, "fetch_raw_data", fake_fetch)
    backfill_result = CliRunner().invoke(
        run_etl.app,
        [
            "backfill",
            "stock-st",
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

    assert load_result.exit_code != 0
    assert backfill_result.exit_code != 0
    assert "暂未实现数据集: dataset=stock-st, source=tushare" in load_result.output
    assert "暂未实现数据集: dataset=stock-st, source=tushare" in backfill_result.output


def test_archive_command_calls_archive_function(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    patch_runtime(monkeypatch, run_etl, tmp_path)
    calls: list[str] = []

    from quant.etl.sources import tushare_source

    def fake_archive(self, dataset, year):
        calls.append(f"archive:{self._config.paths.processed_dir.name}:{dataset}:{year}")
        return (
            self._config.paths.processed_dir
            / dataset
            / f"year={year}"
            / f"{dataset}_{year}.parquet"
        )

    monkeypatch.setattr(tushare_source.TushareSource, "archive_year", fake_archive)

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
    assert calls == ["archive:processed:daily-ohlcv:2025"]
    assert "归档完成:" in result.output


def test_archive_command_supports_adj_factor_and_daily_basic(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    patch_runtime(monkeypatch, run_etl, tmp_path)
    calls: list[str] = []

    from quant.etl.sources import tushare_source

    def fake_archive(self, dataset, year):
        calls.append(f"archive:{dataset}:{year}")
        return (
            self._config.paths.processed_dir
            / dataset
            / f"year={year}"
            / f"{dataset}_{year}.parquet"
        )

    monkeypatch.setattr(tushare_source.TushareSource, "archive_year", fake_archive)

    adj_result = CliRunner().invoke(
        run_etl.app,
        [
            "archive",
            "adj-factor",
            "--source",
            "tushare",
            "--year",
            "2025",
        ],
    )
    basic_result = CliRunner().invoke(
        run_etl.app,
        [
            "archive",
            "daily-basic",
            "--source",
            "tushare",
            "--year",
            "2025",
        ],
    )

    assert adj_result.exit_code == 0
    assert basic_result.exit_code == 0
    assert calls == ["archive:adj-factor:2025", "archive:daily-basic:2025"]


def test_archive_command_rejects_unsupported_tushare_dataset(tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "archive",
            "trade-calendar",
            "--source",
            "tushare",
            "--year",
            "2025",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code != 0
    assert "暂未实现归档: dataset=trade-calendar, source=tushare" in result.output


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

    def fake_status(_config, dataset, *, source=None):
        assert dataset == "trade-calendar"
        assert source == "tushare"
        return {
            "exchange": "SSE",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "row_count": 31,
            "open_count": 22,
        }

    monkeypatch.setattr(run_etl, "get_dataset_status", fake_status)

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

    def fake_status(_config, dataset, *, source=None):
        assert dataset == "daily-ohlcv"
        assert source == "tushare"
        return {
            "start_date": "2024-01-02",
            "end_date": "2024-01-31",
            "row_count": 100,
            "trade_date_count": 22,
            "security_count": 5,
        }

    monkeypatch.setattr(run_etl, "get_dataset_status", fake_status)

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


def test_status_command_reads_adj_factor_processed_state(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    def fake_status(_config, dataset, *, source=None):
        assert dataset == "adj-factor"
        assert source == "tushare"
        return {
            "start_date": "2024-01-02",
            "end_date": "2024-01-31",
            "row_count": 100,
            "trade_date_count": 22,
            "security_count": 5,
        }

    monkeypatch.setattr(run_etl, "get_dataset_status", fake_status)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "status",
            "adj-factor",
            "--source",
            "tushare",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code == 0
    assert "数据集: adj-factor" in result.output
    assert "因子行数: 100" in result.output
    assert "交易日数: 22" in result.output
    assert "证券数: 5" in result.output


def test_status_command_reads_daily_basic_processed_state(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    def fake_status(_config, dataset, *, source=None):
        assert dataset == "daily-basic"
        assert source == "tushare"
        return {
            "start_date": "2024-01-02",
            "end_date": "2024-01-31",
            "row_count": 100,
            "trade_date_count": 22,
            "security_count": 5,
        }

    monkeypatch.setattr(run_etl, "get_dataset_status", fake_status)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "status",
            "daily-basic",
            "--source",
            "tushare",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code == 0
    assert "数据集: daily-basic" in result.output
    assert "指标行数: 100" in result.output
    assert "交易日数: 22" in result.output
    assert "证券数: 5" in result.output


def test_status_command_reads_stock_basic_table(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    def fake_status(_config, dataset, *, source=None):
        assert dataset == "stock-basic"
        assert source == "tushare"
        return {
            "row_count": 5000,
            "exchange_count": 3,
            "listed_count": 4800,
            "delisted_count": 180,
            "paused_count": 20,
        }

    monkeypatch.setattr(run_etl, "get_dataset_status", fake_status)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "status",
            "stock-basic",
            "--source",
            "tushare",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code == 0
    assert "数据集: stock-basic" in result.output
    assert "证券总数: 5000" in result.output
    assert "交易所数量: 3" in result.output
    assert "上市数量: 4800" in result.output
    assert "退市数量: 180" in result.output
    assert "暂停上市数量: 20" in result.output


def test_missing_command_prints_missing_dates(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)
    calls: list[str] = []

    def fake_missing(_config, task):
        calls.append(f"{task.dataset}:{task.source}:{task.start_date}:{task.end_date}")
        return MissingDateResult(
            dataset=task.dataset,
            source=task.source,
            start_date=task.start_date,
            end_date=task.end_date,
            expected_dates=(date(2024, 1, 2), date(2024, 1, 3)),
            existing_dates=(date(2024, 1, 2),),
            missing_dates=(date(2024, 1, 3),),
        )

    monkeypatch.setattr(run_etl, "find_missing_dates", fake_missing)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "missing",
            "daily-ohlcv",
            "--source",
            "tushare",
            "--start-date",
            "20240102",
            "--end-date",
            "20240103",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code == 0
    assert calls == ["daily-ohlcv:tushare:2024-01-02:2024-01-03"]
    assert "数据集: daily-ohlcv" in result.output
    assert "应有日期数: 2" in result.output
    assert "已有日期数: 1" in result.output
    assert "缺失日期数: 1" in result.output
    assert "2024-01-03" in result.output


def test_missing_command_prints_no_missing_dates(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)

    def fake_missing(_config, task):
        return MissingDateResult(
            dataset=task.dataset,
            source=task.source,
            start_date=task.start_date,
            end_date=task.end_date,
            expected_dates=(date(2024, 1, 2),),
            existing_dates=(date(2024, 1, 2),),
            missing_dates=(),
        )

    monkeypatch.setattr(run_etl, "find_missing_dates", fake_missing)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "missing",
            "stock-st",
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

    assert result.exit_code == 0
    assert "缺失日期数: 0" in result.output
    assert "缺失日期: 无" in result.output


def test_daily_command_runs_pipeline_with_explicit_date(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)
    calls: list[str] = []

    def fake_daily_pipeline(_config, source, trade_date, *, force=False, dry_run=False):
        calls.append(f"{source}:{trade_date}:{force}:{dry_run}")
        return DailyPipelineResult(
            trade_date=trade_date,
            source=source,
            is_open=True,
            steps=(
                DailyPipelineStepResult(
                    name="fetch trade-calendar",
                    action="fetch",
                    dataset="trade-calendar",
                    success=True,
                    message="raw_files=1",
                ),
            ),
        )

    monkeypatch.setattr(run_etl, "run_daily_pipeline", fake_daily_pipeline)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "daily",
            "--source",
            "tushare",
            "--date",
            "20260625",
            "--force",
            "--dry-run",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code == 0
    assert calls == ["tushare:2026-06-25:True:True"]
    assert "每日管线完成: date=2026-06-25, source=tushare, is_open=True" in result.output
    assert "步骤: 1, 成功: 1, 失败: 0" in result.output


def test_daily_command_uses_today_when_date_is_omitted(monkeypatch, tmp_path: Path) -> None:
    run_etl = load_run_etl_module()
    config_dir = make_config_dir(tmp_path)
    calls: list[date] = []

    def fake_daily_pipeline(_config, source, trade_date, *, force=False, dry_run=False):
        calls.append(trade_date)
        return DailyPipelineResult(
            trade_date=trade_date,
            source=source,
            is_open=False,
            steps=(
                DailyPipelineStepResult(
                    name="load stock-basic",
                    action="load",
                    dataset="stock-basic",
                    success=True,
                    message="row_count=1",
                ),
            ),
        )

    monkeypatch.setattr(run_etl, "run_daily_pipeline", fake_daily_pipeline)

    result = CliRunner().invoke(
        run_etl.app,
        [
            "daily",
            "--source",
            "tushare",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code == 0
    assert calls == [date.today()]
    assert "当日休市,已跳过日频数据" in result.output
    assert "is_open=False" in result.output


def test_daily_ohlcv_status_reads_processed_view(tmp_path: Path) -> None:
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

    state = get_dataset_status(config, "daily-ohlcv", source="tushare")

    assert state["start_date"] == date(2024, 1, 2)
    assert state["end_date"] == date(2024, 1, 3)
    assert state["row_count"] == 3
    assert state["trade_date_count"] == 2
    assert state["security_count"] == 2


def test_daily_basic_status_reads_processed_view(tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))

    empty_state = get_dataset_status(config, "daily-basic", source="tushare")

    assert empty_state["row_count"] == 0

    parquet_path = (
        config.paths.processed_dir
        / "daily_basic"
        / "year=2024"
        / "month=01"
        / "daily_basic_202401.parquet"
    )
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 2), "close": 10.0},
            {"ts_code": "000002.SZ", "trade_date": date(2024, 1, 2), "close": 20.0},
            {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 3), "close": 10.5},
        ]
    ).to_parquet(parquet_path, index=False)

    state = get_dataset_status(config, "daily-basic", source="tushare")

    assert state["start_date"] == date(2024, 1, 2)
    assert state["end_date"] == date(2024, 1, 3)
    assert state["row_count"] == 3
    assert state["trade_date_count"] == 2
    assert state["security_count"] == 2


def test_adj_factor_status_reads_processed_view(tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    parquet_path = (
        config.paths.processed_dir
        / "adj_factor"
        / "year=2024"
        / "month=01"
        / "adj_factor_202401.parquet"
    )
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 2), "cumulative_factor": 2.0},
            {"ts_code": "000002.SZ", "trade_date": date(2024, 1, 2), "cumulative_factor": 1.0},
            {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 3), "cumulative_factor": 2.1},
        ]
    ).to_parquet(parquet_path, index=False)

    state = get_dataset_status(config, "adj-factor", source="tushare")

    assert state["start_date"] == date(2024, 1, 2)
    assert state["end_date"] == date(2024, 1, 3)
    assert state["row_count"] == 3
    assert state["trade_date_count"] == 2
    assert state["security_count"] == 2


def test_stock_basic_status_reads_dim_security(tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
        conn.executemany(
            """
            INSERT INTO dim_security (ts_code, symbol, name, exchange, list_status)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("000001.SZ", "000001", "平安银行", "SZSE", "L"),
                ("000002.SZ", "000002", "万科A", "SZSE", "D"),
                ("600000.SH", "600000", "浦发银行", "SSE", "P"),
            ],
        )
    state = get_dataset_status(config, "stock-basic", source="tushare")

    assert state["row_count"] == 3
    assert state["exchange_count"] == 2
    assert state["listed_count"] == 1
    assert state["delisted_count"] == 1
    assert state["paused_count"] == 1


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
