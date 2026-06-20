from datetime import date

import pytest
from pydantic import ValidationError

from quant.data.schemas import AdjFactorRecord, DailyOHLCVRecord, FactorRecord, FundamentalRecord


def test_daily_ohlcv_rejects_invalid_ts_code() -> None:
    with pytest.raises(ValidationError, match="ts_code"):
        DailyOHLCVRecord(
            ts_code="000001",
            trade_date=date(2024, 1, 2),
            open=10.0,
            high=11.0,
            low=9.0,
            close=10.5,
            volume=1000.0,
            amount=10500.0,
        )


def test_daily_ohlcv_rejects_invalid_price_and_trading_state() -> None:
    with pytest.raises(ValidationError, match="high 不能低于 low"):
        DailyOHLCVRecord(
            ts_code="000001.SZ",
            trade_date=date(2024, 1, 2),
            open=10.0,
            high=8.0,
            low=9.0,
            close=9.5,
            volume=1000.0,
            amount=9500.0,
        )

    with pytest.raises(ValidationError, match="Input should be"):
        DailyOHLCVRecord(
            ts_code="000001.SZ",
            trade_date=date(2024, 1, 2),
            open=10.0,
            high=11.0,
            low=9.0,
            close=10.5,
            volume=1000.0,
            amount=10500.0,
            limit_status="locked",
        )


def test_daily_ohlcv_rejects_negative_volume() -> None:
    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        DailyOHLCVRecord(
            ts_code="000001.SZ",
            trade_date=date(2024, 1, 2),
            open=10.0,
            high=11.0,
            low=9.0,
            close=10.5,
            volume=-1.0,
            amount=10500.0,
        )


def test_adj_factor_record_uses_standard_cumulative_factor() -> None:
    record = AdjFactorRecord(
        ts_code="000001.SZ",
        trade_date=date(2024, 1, 2),
        cumulative_factor=2.0,
    )

    assert record.cumulative_factor == 2.0

    with pytest.raises(ValidationError, match="greater than 0"):
        AdjFactorRecord(
            ts_code="000001.SZ",
            trade_date=date(2024, 1, 2),
            cumulative_factor=0.0,
        )


def test_fundamental_record_preserves_announcement_and_report_dates() -> None:
    record = FundamentalRecord(
        ts_code="000001.SZ",
        ann_date=date(2024, 4, 30),
        report_date=date(2024, 3, 31),
        roe=0.12,
    )

    assert record.ann_date == date(2024, 4, 30)
    assert record.report_date == date(2024, 3, 31)


def test_factor_record_defaults_factor_version() -> None:
    record = FactorRecord(
        ts_code="000001.SZ",
        trade_date=date(2024, 1, 2),
        factor_name="momentum_20d",
        factor_value=1.23,
    )

    assert record.factor_version == "default"
