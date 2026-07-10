"""流动性等另类因子元数据。"""

from quant.features.base import FactorMetadata

ALTERNATIVE_FACTORS = (
    FactorMetadata(
        name="log_amount_mean_20d",
        version="v1",
        category="alternative",
        lookback_days=20,
        required_fields=("amount",),
        min_periods=20,
        higher_is_better=None,
        description="过去 20 个交易日平均成交额的自然对数。",
    ),
    FactorMetadata(
        name="amihud_20d",
        version="v1",
        category="alternative",
        lookback_days=20,
        required_fields=("hfq_close", "amount"),
        min_periods=20,
        higher_is_better=False,
        description="过去 20 个交易日单位成交额对应的绝对收益率均值。",
    ),
)
