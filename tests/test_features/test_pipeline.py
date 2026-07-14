from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from quant.config import QuantConfig, load_config
from quant.data.db import DuckDBManager
from quant.data.repository import QuantRepository
from quant.features import run_factor_pipeline


@pytest.mark.parametrize(
    ("factor_names", "start_date", "end_date", "processor", "error_message"),
    [
        ([], date(2024, 1, 10), date(2024, 1, 10), "raw", "factor_names 不能为空"),
        (
            ["return_5d", "return_5d"],
            date(2024, 1, 10),
            date(2024, 1, 10),
            "raw",
            "factor_names 不能重复",
        ),
        (
            ["return_5d"],
            date(2024, 1, 11),
            date(2024, 1, 10),
            "raw",
            "start_date 不能晚于 end_date",
        ),
        (
            ["return_5d"],
            date(2024, 1, 10),
            date(2024, 1, 10),
            "zscore",
            "不支持的因子 Processor: zscore",
        ),
        (
            "return_5d",
            date(2024, 1, 10),
            date(2024, 1, 10),
            "raw",
            "factor_names 必须是因子名称序列,不能传入字符串",
        ),
    ],
)
def test_run_factor_pipeline_validates_task_before_io(
    tmp_path: Path,
    factor_names: list[str] | str,
    start_date: date,
    end_date: date,
    processor: str,
    error_message: str,
) -> None:
    """无效任务参数不依赖数据文件即可返回清晰错误。"""
    config = make_config(tmp_path)

    with pytest.raises(ValueError, match=error_message):
        run_factor_pipeline(
            config,
            factor_names,
            start_date,
            end_date,
            processor=processor,  # type: ignore[arg-type]
        )


def test_run_factor_pipeline_rejects_unknown_and_unimplemented_factors(tmp_path: Path) -> None:
    """未知因子和只有元数据的因子使用不同错误语义。"""
    config = make_config(tmp_path)

    with pytest.raises(KeyError, match="未注册因子: name=unknown, version=v1"):
        run_factor_pipeline(config, ["unknown"], date(2024, 1, 10), date(2024, 1, 10))

    with pytest.raises(
        NotImplementedError,
        match="因子 Calculator 尚未实现: name=momentum_20d, version=v1",
    ):
        run_factor_pipeline(config, ["momentum_20d"], date(2024, 1, 10), date(2024, 1, 10))


def test_run_factor_pipeline_requires_enough_trade_calendar_history(tmp_path: Path) -> None:
    """warmup 不使用自然日近似,交易日历不足时直接失败。"""
    config = make_config(tmp_path)
    requested_date = date(2024, 1, 10)
    initialize_calendar(config, [requested_date - timedelta(days=offset) for offset in range(1, 5)])

    with pytest.raises(ValueError, match="交易日历不足以计算 warmup: required=5, available=4"):
        run_factor_pipeline(config, ["return_5d"], requested_date, requested_date)


def test_run_factor_pipeline_rejects_empty_daily_panel(tmp_path: Path) -> None:
    """输入视图存在但请求区间没有行情时直接失败。"""
    config = make_config(tmp_path)
    requested_date = date(2024, 1, 10)
    trade_dates = [requested_date - timedelta(days=offset) for offset in range(5, 0, -1)]
    write_market_data(config.paths.processed_dir, [date(2023, 1, 2)])
    initialize_calendar(config, trade_dates)

    with pytest.raises(ValueError, match="因子输入面板为空"):
        run_factor_pipeline(config, ["return_5d"], requested_date, requested_date)


def test_run_factor_pipeline_rejects_request_range_without_factor_rows(tmp_path: Path) -> None:
    """只有 warmup 行情但请求日本身无行情时不写空结果。"""
    config, trade_dates = make_initialized_factor_data(tmp_path)
    requested_date = trade_dates[-1] + timedelta(days=1)

    with pytest.raises(ValueError, match="请求区间没有因子结果"):
        run_factor_pipeline(config, ["return_5d"], requested_date, requested_date)


def test_run_factor_pipeline_writes_raw_result_and_refreshes_view(tmp_path: Path) -> None:
    """Pipeline 读取 warmup、只写请求区间并立即允许 Repository 查询。"""
    config, trade_dates = make_initialized_factor_data(tmp_path)

    summary = run_factor_pipeline(
        config,
        ["return_5d"],
        trade_dates[-1],
        trade_dates[-1],
    )

    expected_path = (
        config.paths.processed_dir / "factors" / "year=2024" / "month=01" / "factors_202401.parquet"
    )
    assert summary.factor_names == ("return_5d",)
    assert summary.warmup_start_date == trade_dates[0]
    assert summary.input_row_count == 12
    assert summary.output_row_count == 2
    assert summary.written_paths == (expected_path,)
    assert summary.missing_value_rate == 0.0
    assert summary.valid_rate == 1.0
    assert summary.processor == "raw"
    assert summary.dry_run is False

    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    with manager.session() as conn:
        rows = QuantRepository(conn).get_factors(
            trade_dates[-1],
            ["return_5d"],
            factor_version="v1",
        )

    assert [row["factor_value"] for row in rows] == pytest.approx([0.5, 1.0])
    stored = pd.read_parquet(expected_path)
    assert stored["trade_date"].tolist() == [trade_dates[-1], trade_dates[-1]]
    assert stored["quality_status"].tolist() == ["valid", "valid"]


def test_run_factor_pipeline_supports_rank_pct(tmp_path: Path) -> None:
    """rank_pct 写入请求日的截面百分位排名并保留 raw_value。"""
    config, trade_dates = make_initialized_factor_data(tmp_path)

    summary = run_factor_pipeline(
        config,
        ["return_5d"],
        trade_dates[-1],
        trade_dates[-1],
        processor="rank_pct",
    )

    output_path = summary.written_paths[0]
    stored = pd.read_parquet(output_path).sort_values("ts_code")
    assert stored["raw_value"].tolist() == pytest.approx([0.5, 1.0])
    assert stored["processed_value"].tolist() == pytest.approx([0.5, 1.0])
    assert stored["factor_value"].tolist() == pytest.approx([0.5, 1.0])


def test_run_factor_pipeline_dry_run_does_not_write_or_refresh(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """dry-run 完成计算和摘要,但不创建因子数据集。"""
    config, trade_dates = make_initialized_factor_data(tmp_path)
    refresh_calls: list[bool] = []
    monkeypatch.setattr(
        DuckDBManager,
        "refresh_views",
        lambda _manager: refresh_calls.append(True),
    )

    summary = run_factor_pipeline(
        config,
        ["return_5d"],
        trade_dates[-1],
        trade_dates[-1],
        dry_run=True,
    )

    assert summary.output_row_count == 2
    assert summary.written_paths == ()
    assert summary.dry_run is True
    assert not (config.paths.processed_dir / "factors").exists()
    assert refresh_calls == []


def make_initialized_factor_data(tmp_path: Path) -> tuple[QuantConfig, list[date]]:
    """构造包含六个交易日和两只股票的 Pipeline 测试数据。"""
    config = make_config(tmp_path)
    trade_dates = [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
        date(2024, 1, 9),
    ]
    write_market_data(config.paths.processed_dir, trade_dates)
    initialize_calendar(config, trade_dates)
    return config, trade_dates


def initialize_calendar(config: QuantConfig, trade_dates: list[date]) -> None:
    """初始化 DuckDB 并写入测试交易日历。"""
    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO dim_trade_calendar
                (exchange, cal_date, is_open, pretrade_date)
            VALUES ('SSE', ?, TRUE, NULL)
            """,
            [(trade_date,) for trade_date in trade_dates],
        )


def write_market_data(processed_dir: Path, trade_dates: list[date]) -> None:
    """写入两只股票的最小 OHLCV 和复权因子数据。"""
    first_closes = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0][: len(trade_dates)]
    second_closes = [20.0, 22.0, 24.0, 26.0, 28.0, 40.0][: len(trade_dates)]
    rows = [
        (ts_code, trade_date, close)
        for ts_code, closes in (("000001.SZ", first_closes), ("000002.SZ", second_closes))
        for trade_date, close in zip(trade_dates, closes, strict=True)
    ]
    ohlcv_path = processed_dir / "ohlcv" / "year=2024" / "month=01" / "bars.parquet"
    ohlcv_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_code": [row[0] for row in rows],
            "trade_date": [row[1] for row in rows],
            "open": [row[2] for row in rows],
            "high": [row[2] for row in rows],
            "low": [row[2] for row in rows],
            "close": [row[2] for row in rows],
            "pre_close": [row[2] for row in rows],
            "change": [0.0] * len(rows),
            "pct_chg": [0.0] * len(rows),
            "volume": [1000.0] * len(rows),
            "amount": [10000.0] * len(rows),
            "is_suspended": [False] * len(rows),
            "is_st": [False] * len(rows),
            "limit_status": [0] * len(rows),
        }
    ).write_parquet(ohlcv_path)

    adj_path = processed_dir / "adj_factor" / "year=2024" / "month=01" / "adj.parquet"
    adj_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_code": [row[0] for row in rows],
            "trade_date": [row[1] for row in rows],
            "cumulative_factor": [1.0] * len(rows),
        }
    ).write_parquet(adj_path)


def make_config(tmp_path: Path) -> QuantConfig:
    """创建隔离的项目配置。"""
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
