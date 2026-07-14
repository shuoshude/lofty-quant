from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from quant.data.db import DuckDBManager
from quant.data.repository import QuantRepository
from quant.features import build_default_registry, compute_return_5d, write_factor_results


def test_compute_return_5d_keeps_warmup_rows_and_calculates_exact_return() -> None:
    """前五个观测保留为空,第六个观测产生原始五日收益。"""
    panel = pl.DataFrame(
        {
            "ts_code": ["000001.SZ"] * 6,
            "trade_date": [date(2024, 1, 2) + timedelta(days=offset) for offset in range(6)],
            "hfq_close": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
        }
    )

    result = compute_return_5d(panel)

    assert result.columns == [
        "ts_code",
        "trade_date",
        "factor_name",
        "factor_value",
        "factor_version",
        "raw_value",
    ]
    assert result["raw_value"].head(5).null_count() == 5
    assert result["factor_value"].head(5).null_count() == 5
    assert result["raw_value"][-1] == pytest.approx(0.5)
    assert result["factor_value"][-1] == pytest.approx(0.5)
    assert result["factor_name"].unique().to_list() == ["return_5d"]
    assert result["factor_version"].unique().to_list() == ["v1"]


def test_compute_return_5d_sorts_input_and_shifts_within_each_stock() -> None:
    """乱序和交错股票不会改变输出顺序或造成跨股票位移。"""
    trade_dates = [date(2024, 1, 2) + timedelta(days=offset) for offset in range(6)]
    panel = pl.DataFrame(
        {
            "ts_code": ["000002.SZ", "000001.SZ"] * 6,
            "trade_date": [item for trade_date in trade_dates for item in (trade_date, trade_date)],
            "hfq_close": [
                item
                for first_close, second_close in zip(
                    [20.0, 22.0, 24.0, 26.0, 28.0, 30.0],
                    [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
                    strict=True,
                )
                for item in (first_close, second_close)
            ],
        }
    ).reverse()

    result = compute_return_5d(panel)

    assert result.select("ts_code", "trade_date").to_dicts() == [
        {"ts_code": ts_code, "trade_date": trade_date}
        for ts_code in ("000001.SZ", "000002.SZ")
        for trade_date in trade_dates
    ]
    assert result.filter(pl.col("ts_code") == "000001.SZ")["raw_value"][-1] == pytest.approx(0.5)
    assert result.filter(pl.col("ts_code") == "000002.SZ")["raw_value"][-1] == pytest.approx(0.5)
    null_counts = result.group_by("ts_code").agg(pl.col("raw_value").null_count()).sort("ts_code")
    assert null_counts.to_dicts() == [
        {"ts_code": "000001.SZ", "raw_value": 5},
        {"ts_code": "000002.SZ", "raw_value": 5},
    ]


@pytest.mark.parametrize("missing_column", ["ts_code", "trade_date", "hfq_close"])
def test_compute_return_5d_rejects_missing_required_columns(missing_column: str) -> None:
    """输入字段错误在计算前报告具体缺失字段。"""
    panel = pl.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [date(2024, 1, 2)],
            "hfq_close": [10.0],
        }
    ).drop(missing_column)

    with pytest.raises(ValueError, match=rf"return_5d 输入缺少字段: \['{missing_column}'\]"):
        compute_return_5d(panel)


def test_return_5d_metadata_keeps_raw_return_direction() -> None:
    """原始收益不反向,评价方向由元数据单独表达。"""
    metadata = build_default_registry().get("return_5d")

    assert metadata.higher_is_better is False
    assert metadata.required_fields == ("hfq_close",)
    assert metadata.lookback_days == 5


def test_return_5d_runs_through_repository_storage_and_query(tmp_path: Path) -> None:
    """五日收益可以从研究面板计算、落盘并由 Repository 查询。"""
    processed_dir = tmp_path / "processed"
    trade_dates = [date(2024, 1, 2) + timedelta(days=offset) for offset in range(6)]
    _write_market_data(processed_dir, trade_dates)
    manager = DuckDBManager(tmp_path / "quant.duckdb", processed_dir)
    manager.initialize()

    with manager.session() as conn:
        panel = QuantRepository(conn).get_daily_panel(
            trade_dates[0],
            trade_dates[-1],
            ["hfq_close"],
        )

    result = compute_return_5d(panel)
    write_factor_results(processed_dir, result.to_pandas())
    manager.initialize()

    with manager.session() as conn:
        rows = QuantRepository(conn).get_factors(
            trade_dates[-1],
            ["return_5d"],
            factor_version="v1",
        )

    assert rows == [
        {
            "ts_code": "000001.SZ",
            "trade_date": trade_dates[-1],
            "factor_name": "return_5d",
            "factor_value": pytest.approx(0.5),
            "factor_version": "v1",
        }
    ]


def _write_market_data(processed_dir: Path, trade_dates: list[date]) -> None:
    """写入端到端测试所需的最小行情和复权因子。"""
    closes = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
    ohlcv_path = processed_dir / "ohlcv" / "year=2024" / "month=01" / "bars.parquet"
    ohlcv_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_code": ["000001.SZ"] * 6,
            "trade_date": trade_dates,
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "pre_close": closes,
            "change": [0.0] * 6,
            "pct_chg": [0.0] * 6,
            "volume": [1000.0] * 6,
            "amount": [10000.0] * 6,
            "is_suspended": [False] * 6,
            "is_st": [False] * 6,
            "limit_status": [0] * 6,
        }
    ).write_parquet(ohlcv_path)

    adj_factor_path = processed_dir / "adj_factor" / "year=2024" / "month=01" / "adj.parquet"
    adj_factor_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_code": ["000001.SZ"] * 6,
            "trade_date": trade_dates,
            "cumulative_factor": [1.0] * 6,
        }
    ).write_parquet(adj_factor_path)
