"""因子评价使用的未来收益标签。"""

from collections.abc import Sequence
from datetime import date

import polars as pl

PRICE_PANEL_COLUMNS = ("ts_code", "trade_date", "hfq_open")
PRICE_KEY_COLUMNS = ("ts_code", "trade_date")
FORWARD_RETURN_5D_COLUMNS = ("ts_code", "trade_date", "forward_return_5d")


def compute_forward_return_5d(
    price_panel: pl.DataFrame,
    trade_dates: Sequence[date],
) -> pl.DataFrame:
    """按交易所 T+1 开盘买入、T+6 开盘卖出计算五日未来收益。"""
    missing_columns = [
        column for column in PRICE_PANEL_COLUMNS if column not in price_panel.columns
    ]
    if missing_columns:
        raise ValueError(f"未来收益输入缺少字段: {missing_columns}")
    if price_panel.select(PRICE_KEY_COLUMNS).is_duplicated().any():
        raise ValueError(f"未来收益输入包含重复键: key_columns={PRICE_KEY_COLUMNS}")

    normalized_trade_dates = tuple(trade_dates)
    if not normalized_trade_dates:
        raise ValueError("trade_dates 不能为空")
    if len(set(normalized_trade_dates)) != len(normalized_trade_dates):
        raise ValueError("trade_dates 不能重复")
    ordered_trade_dates = tuple(sorted(normalized_trade_dates))

    date_mapping = pl.DataFrame(
        {
            "trade_date": ordered_trade_dates,
            "_entry_date": [
                ordered_trade_dates[index + 1] if index + 1 < len(ordered_trade_dates) else None
                for index in range(len(ordered_trade_dates))
            ],
            "_exit_date": [
                ordered_trade_dates[index + 6] if index + 6 < len(ordered_trade_dates) else None
                for index in range(len(ordered_trade_dates))
            ],
        },
        schema={"trade_date": pl.Date, "_entry_date": pl.Date, "_exit_date": pl.Date},
    )
    entry_prices = price_panel.select(
        "ts_code",
        pl.col("trade_date").alias("_entry_date"),
        pl.col("hfq_open").alias("_entry_open"),
    )
    exit_prices = price_panel.select(
        "ts_code",
        pl.col("trade_date").alias("_exit_date"),
        pl.col("hfq_open").alias("_exit_open"),
    )

    joined = (
        price_panel.select("ts_code", "trade_date")
        .join(date_mapping, on="trade_date", how="left")
        .join(entry_prices, on=["ts_code", "_entry_date"], how="left")
        .join(exit_prices, on=["ts_code", "_exit_date"], how="left")
        .with_columns((pl.col("_exit_open") / pl.col("_entry_open") - 1).alias("_return"))
    )
    return (
        joined.with_columns(
            pl.when(pl.col("_return").is_finite())
            .then(pl.col("_return"))
            .otherwise(None)
            .alias("forward_return_5d")
        )
        .select(FORWARD_RETURN_5D_COLUMNS)
        .sort(PRICE_KEY_COLUMNS)
    )
