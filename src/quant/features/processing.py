"""因子原始结果的轻量处理。"""

from typing import Literal

import polars as pl

from quant.features.base import FactorMetadata

FactorProcessor = Literal["raw", "rank_pct"]

CALCULATOR_RESULT_COLUMNS = (
    "ts_code",
    "trade_date",
    "factor_name",
    "factor_value",
    "factor_version",
    "raw_value",
)
PROCESSED_FACTOR_COLUMNS = (
    "ts_code",
    "trade_date",
    "factor_name",
    "factor_value",
    "factor_version",
    "raw_value",
    "processed_value",
    "quality_status",
)


def process_factor_result(
    result: pl.DataFrame,
    metadata: FactorMetadata,
    processor: FactorProcessor,
) -> pl.DataFrame:
    """标记原始因子质量并生成当前可消费因子值。"""
    if processor not in ("raw", "rank_pct"):
        raise ValueError(f"不支持的因子 Processor: {processor}")

    missing_columns = [
        column for column in CALCULATOR_RESULT_COLUMNS if column not in result.columns
    ]
    if missing_columns:
        raise ValueError(f"Calculator 结果缺少字段: {missing_columns}")

    expected_identity = {(metadata.name, metadata.version)}
    actual_identities = set(result.select("factor_name", "factor_version").unique().iter_rows())
    if actual_identities != expected_identity:
        displayed_identities = sorted(
            (repr(name), repr(version)) for name, version in actual_identities
        )
        raise ValueError(
            "Calculator 结果因子身份不匹配: "
            f"expected={metadata.name}:{metadata.version}, actual={displayed_identities}"
        )

    prepared = result.sort(["ts_code", "trade_date"]).with_columns(
        pl.col("trade_date").cum_count().over("ts_code").alias("_observation_count")
    )
    prepared = prepared.with_columns(
        pl.when(pl.col("raw_value").is_null())
        .then(
            pl.when(pl.col("_observation_count") < metadata.min_periods)
            .then(pl.lit("insufficient_history"))
            .otherwise(pl.lit("missing_input"))
        )
        .when(~pl.col("raw_value").is_finite())
        .then(pl.lit("invalid_value"))
        .otherwise(pl.lit("valid"))
        .alias("quality_status")
    ).with_columns(
        pl.when(pl.col("quality_status") == "valid")
        .then(pl.col("raw_value"))
        .otherwise(None)
        .alias("_valid_raw_value")
    )

    if processor == "raw":
        processed = prepared.with_columns(
            pl.col("_valid_raw_value").alias("factor_value"),
            pl.lit(None, dtype=pl.Float64).alias("processed_value"),
        )
    else:
        rank = pl.col("_valid_raw_value").rank(method="average").over("trade_date")
        valid_count = pl.col("_valid_raw_value").count().over("trade_date")
        processed = prepared.with_columns(
            (rank / valid_count).alias("processed_value")
        ).with_columns(
            pl.col("processed_value").alias("factor_value"),
        )

    return processed.select(PROCESSED_FACTOR_COLUMNS)
