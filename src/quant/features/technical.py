"""技术类因子元数据。"""

from quant.features.base import FactorMetadata

TECHNICAL_FACTORS = (
    FactorMetadata(
        name="return_5d",
        version="v1",
        category="technical",
        lookback_days=5,
        required_fields=("hfq_close",),
        min_periods=6,
        higher_is_better=False,
        description="过去 5 个交易日的后复权收盘价收益率。",
    ),
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
