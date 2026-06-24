from datetime import date

import pytest
from pydantic import ValidationError

from quant.data.schemas import (
    AdjFactorRecord,
    DailyBasicRecord,
    DailyOHLCVRecord,
    FactorRecord,
    FundamentalRecord,
    SecurityRecord,
)


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


def test_daily_ohlcv_schema_contains_chinese_field_descriptions() -> None:
    schema = DailyOHLCVRecord.model_json_schema()

    assert schema["properties"]["trade_date"]["description"] == "交易日"
    assert schema["properties"]["close"]["description"] == "收盘价"
    assert schema["properties"]["limit_status"]["description"] == "涨跌停状态"


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


def test_daily_basic_schema_contains_official_field_descriptions() -> None:
    schema = DailyBasicRecord.model_json_schema()

    assert schema["properties"]["close"]["description"] == "当日收盘价"
    assert schema["properties"]["turnover_rate_f"]["description"] == "自由流通股换手率"
    assert schema["properties"]["dv_ratio"]["description"] == "股息率"
    assert schema["properties"]["dv_ttm"]["description"] == "滚动股息率"


def test_daily_basic_rejects_invalid_ts_code_and_negative_values() -> None:
    record = DailyBasicRecord(
        ts_code="000001.SZ",
        trade_date=date(2024, 1, 2),
        pe=-1.0,
        pe_ttm=-1.0,
        dv_ratio=0.0,
        dv_ttm=0.0,
    )

    assert record.pe == -1.0
    assert record.pe_ttm == -1.0
    assert record.volume_ratio is None

    with pytest.raises(ValidationError, match="ts_code"):
        DailyBasicRecord(
            ts_code="000001",
            trade_date=date(2024, 1, 2),
        )

    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        DailyBasicRecord(
            ts_code="000001.SZ",
            trade_date=date(2024, 1, 2),
            turnover_rate=-1.0,
        )

    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        DailyBasicRecord(
            ts_code="000001.SZ",
            trade_date=date(2024, 1, 2),
            total_mv=-1.0,
        )

    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        DailyBasicRecord(
            ts_code="000001.SZ",
            trade_date=date(2024, 1, 2),
            volume_ratio=-1.0,
        )

    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        DailyBasicRecord(
            ts_code="000001.SZ",
            trade_date=date(2024, 1, 2),
            dv_ratio=-1.0,
            dv_ttm=-1.0,
        )


def test_security_record_matches_stock_basic_contract() -> None:
    record = SecurityRecord(
        ts_code="000001.SZ",
        symbol="000001",
        name="平安银行",
        area="深圳",
        industry="银行",
        fullname="平安银行股份有限公司",
        enname="Ping An Bank Co., Ltd.",
        cnspell="payh",
        market="主板",
        exchange="SZSE",
        curr_type="CNY",
        list_status="L",
        list_date="19910403",
        delist_date=None,
        is_hs="S",
        act_name="无",
        act_ent_type="无",
    )

    schema = SecurityRecord.model_json_schema()

    assert record.list_status == "L"
    assert schema["properties"]["fullname"]["description"] == "证券全称"
    assert (
        schema["properties"]["list_status"]["description"]
        == "上市状态, L=上市, D=退市, P=暂停上市"
    )

    with pytest.raises(ValidationError, match="Input should be"):
        SecurityRecord(ts_code="000001.SZ", list_status="A")


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
