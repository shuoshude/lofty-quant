from datetime import date, timedelta

import polars as pl
import pytest

from quant.features import build_default_registry
from quant.features.processing import process_factor_result


def test_process_factor_result_raw_preserves_values_and_marks_quality() -> None:
    """raw 处理保留有效原值并区分 warmup、输入缺失和非法值。"""
    result = make_result([None, None, None, None, None, 0.5, None, float("inf")])
    metadata = build_default_registry().get("return_5d")

    processed = process_factor_result(result, metadata, "raw")

    assert processed.columns == [
        "ts_code",
        "trade_date",
        "factor_name",
        "factor_value",
        "factor_version",
        "raw_value",
        "processed_value",
        "quality_status",
    ]
    assert processed["quality_status"].to_list() == [
        "insufficient_history",
        "insufficient_history",
        "insufficient_history",
        "insufficient_history",
        "insufficient_history",
        "valid",
        "missing_input",
        "invalid_value",
    ]
    assert processed["factor_value"].to_list()[:7] == [None, None, None, None, None, 0.5, None]
    assert processed["factor_value"][-1] is None
    assert processed["processed_value"].null_count() == processed.height


def test_process_factor_result_rank_pct_ranks_each_date_with_average_ties() -> None:
    """rank_pct 只在同日有效值中计算平均百分位排名。"""
    trade_date = date(2024, 1, 10)
    result = pl.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"],
            "trade_date": [trade_date] * 4,
            "factor_name": ["return_5d"] * 4,
            "factor_value": [0.1, 0.2, 0.2, None],
            "factor_version": ["v1"] * 4,
            "raw_value": [0.1, 0.2, 0.2, None],
        }
    )
    metadata = build_default_registry().get("return_5d")

    processed = process_factor_result(result, metadata, "rank_pct")

    assert processed["processed_value"].to_list()[:3] == pytest.approx([1 / 3, 5 / 6, 5 / 6])
    assert processed["processed_value"][-1] is None
    assert processed["factor_value"].to_list()[:3] == pytest.approx([1 / 3, 5 / 6, 5 / 6])


def test_process_factor_result_rank_pct_excludes_non_finite_values() -> None:
    """非有限值标记为 invalid_value 且不进入排名分母。"""
    trade_date = date(2024, 1, 10)
    result = pl.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": [trade_date] * 2,
            "factor_name": ["return_5d"] * 2,
            "factor_value": [0.1, float("inf")],
            "factor_version": ["v1"] * 2,
            "raw_value": [0.1, float("inf")],
        }
    )
    metadata = build_default_registry().get("return_5d")

    processed = process_factor_result(result, metadata, "rank_pct")

    assert processed["factor_value"].to_list() == [1.0, None]
    assert processed["quality_status"].to_list() == ["valid", "invalid_value"]


def test_process_factor_result_rejects_calculator_identity_mismatch() -> None:
    """Calculator 输出的名称和版本必须与请求元数据一致。"""
    metadata = build_default_registry().get("return_5d")
    result = make_result([0.1]).with_columns(pl.lit("wrong_name").alias("factor_name"))

    with pytest.raises(
        ValueError,
        match="Calculator 结果因子身份不匹配: expected=return_5d:v1",
    ):
        process_factor_result(result, metadata, "raw")


def test_process_factor_result_rank_pct_does_not_mix_dates() -> None:
    """不同日期各自形成独立截面。"""
    metadata = build_default_registry().get("return_5d")
    result = pl.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000001.SZ", "000002.SZ"],
            "trade_date": [date(2024, 1, 10)] * 2 + [date(2024, 1, 11)] * 2,
            "factor_name": ["return_5d"] * 4,
            "factor_value": [0.1, 0.2, 0.4, 0.3],
            "factor_version": ["v1"] * 4,
            "raw_value": [0.1, 0.2, 0.4, 0.3],
        }
    )

    processed = process_factor_result(result, metadata, "rank_pct")

    assert processed["factor_value"].to_list() == pytest.approx([0.5, 1.0, 1.0, 0.5])


def test_process_factor_result_rejects_unknown_processor() -> None:
    """未知处理方式在处理前失败。"""
    metadata = build_default_registry().get("return_5d")

    with pytest.raises(ValueError, match="不支持的因子 Processor: zscore"):
        process_factor_result(make_result([None]), metadata, "zscore")  # type: ignore[arg-type]


def make_result(raw_values: list[float | None]) -> pl.DataFrame:
    """构造单股票 Calculator 结果。"""
    return pl.DataFrame(
        {
            "ts_code": ["000001.SZ"] * len(raw_values),
            "trade_date": [
                date(2024, 1, 2) + timedelta(days=offset) for offset in range(len(raw_values))
            ],
            "factor_name": ["return_5d"] * len(raw_values),
            "factor_value": raw_values,
            "factor_version": ["v1"] * len(raw_values),
            "raw_value": raw_values,
        }
    )
