"""技术类因子元数据和计算函数。"""

from collections.abc import Callable, Mapping
from types import MappingProxyType

import polars as pl

from quant.features.base import FactorMetadata

_RETURN_5D_METADATA = FactorMetadata(
    name="return_5d",
    version="v1",
    category="technical",
    lookback_days=5,
    required_fields=("hfq_close",),
    min_periods=6,
    higher_is_better=False,
    description="过去 5 个交易日的后复权收盘价收益率。",
)

TECHNICAL_FACTORS = (
    _RETURN_5D_METADATA,
    FactorMetadata(
        name="momentum_20d",
        version="v1",
        category="technical",
        lookback_days=20,
        required_fields=("hfq_close",),
        min_periods=21,
        higher_is_better=True,
        description="过去 20 个交易日的后复权收盘价动量收益率。",
    ),
    FactorMetadata(
        name="volatility_20d",
        version="v1",
        category="technical",
        lookback_days=20,
        required_fields=("hfq_close",),
        min_periods=20,
        higher_is_better=False,
        description="过去 20 个交易日的日对数收益率波动率,不做年化。",
    ),
)


def compute_return_5d(panel: pl.DataFrame) -> pl.DataFrame:
    """计算每只股票的五日后复权收盘价原始收益。"""
    required_columns = ("ts_code", "trade_date", *_RETURN_5D_METADATA.required_fields)
    missing_columns = [column for column in required_columns if column not in panel.columns]
    if missing_columns:
        raise ValueError(f"return_5d 输入缺少字段: {missing_columns}")

    with_raw_value = panel.sort(["ts_code", "trade_date"]).with_columns(
        (
            pl.col("hfq_close")
            / pl.col("hfq_close").shift(_RETURN_5D_METADATA.lookback_days).over("ts_code")
            - 1
        ).alias("raw_value")
    )
    return with_raw_value.with_columns(
        pl.lit(_RETURN_5D_METADATA.name).alias("factor_name"),
        pl.col("raw_value").alias("factor_value"),
        pl.lit(_RETURN_5D_METADATA.version).alias("factor_version"),
    ).select(
        "ts_code",
        "trade_date",
        "factor_name",
        "factor_value",
        "factor_version",
        "raw_value",
    )


FactorCalculator = Callable[[pl.DataFrame], pl.DataFrame]

TECHNICAL_CALCULATORS: Mapping[tuple[str, str], FactorCalculator] = MappingProxyType(
    {(_RETURN_5D_METADATA.name, _RETURN_5D_METADATA.version): compute_return_5d}
)
