from datetime import date

import polars as pl
import pytest

from quant.features import compute_forward_return_5d


def test_compute_forward_return_5d_uses_exchange_trade_date_offsets() -> None:
    """标签使用交易日历中的 T+1 和 T+6,不受周末间隔影响。"""
    trade_dates = make_trade_dates()
    panel = pl.DataFrame(
        {
            "ts_code": ["000001.SZ"] * 7,
            "trade_date": trade_dates,
            "hfq_open": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0],
        }
    )

    labels = compute_forward_return_5d(panel, trade_dates)

    assert labels.columns == ["ts_code", "trade_date", "forward_return_5d"]
    assert labels["forward_return_5d"][0] == pytest.approx(16.0 / 11.0 - 1)
    assert labels["forward_return_5d"].tail(6).null_count() == 6


def test_compute_forward_return_5d_does_not_compress_missing_stock_rows() -> None:
    """股票缺少目标交易日行情时标签为空,不会按下一条个股记录位移。"""
    trade_dates = make_trade_dates()
    panel = pl.DataFrame(
        {
            "ts_code": ["000001.SZ"] * 6,
            "trade_date": [trade_dates[0], *trade_dates[2:]],
            "hfq_open": [10.0, 12.0, 13.0, 14.0, 15.0, 16.0],
        }
    )

    labels = compute_forward_return_5d(panel, trade_dates)

    assert labels.filter(pl.col("trade_date") == trade_dates[0])["forward_return_5d"][0] is None


def test_compute_forward_return_5d_nulls_non_finite_or_missing_prices() -> None:
    """缺失价格和除零产生的非有限收益不会进入标签。"""
    trade_dates = make_trade_dates()
    panel = pl.DataFrame(
        {
            "ts_code": ["000001.SZ"] * 7 + ["000002.SZ"] * 7,
            "trade_date": trade_dates * 2,
            "hfq_open": [
                *[10.0, None, 12.0, 13.0, 14.0, 15.0, 16.0],
                *[10.0, 0.0, 12.0, 13.0, 14.0, 15.0, 16.0],
            ],
        }
    )

    labels = compute_forward_return_5d(panel, trade_dates)

    first_date = labels.filter(pl.col("trade_date") == trade_dates[0])
    assert first_date["forward_return_5d"].null_count() == 2


@pytest.mark.parametrize(
    ("panel", "trade_dates", "error_message"),
    [
        (
            pl.DataFrame({"ts_code": ["000001.SZ"], "trade_date": [date(2024, 1, 2)]}),
            [date(2024, 1, 2)],
            r"未来收益输入缺少字段: \['hfq_open'\]",
        ),
        (
            pl.DataFrame(
                {
                    "ts_code": ["000001.SZ", "000001.SZ"],
                    "trade_date": [date(2024, 1, 2), date(2024, 1, 2)],
                    "hfq_open": [10.0, 10.0],
                }
            ),
            [date(2024, 1, 2)],
            "未来收益输入包含重复键",
        ),
        (
            pl.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": [date(2024, 1, 2)],
                    "hfq_open": [10.0],
                }
            ),
            [],
            "trade_dates 不能为空",
        ),
    ],
)
def test_compute_forward_return_5d_validates_input(
    panel: pl.DataFrame,
    trade_dates: list[date],
    error_message: str,
) -> None:
    """标签计算在连接前校验字段、键和交易日历。"""
    with pytest.raises(ValueError, match=error_message):
        compute_forward_return_5d(panel, trade_dates)


def make_trade_dates() -> list[date]:
    """构造跨周末的七个交易日。"""
    return [
        date(2024, 1, 5),
        date(2024, 1, 8),
        date(2024, 1, 9),
        date(2024, 1, 10),
        date(2024, 1, 11),
        date(2024, 1, 12),
        date(2024, 1, 15),
    ]
