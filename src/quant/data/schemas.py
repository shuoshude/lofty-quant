"""Validated data contracts for A-share datasets."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

TsCode = str
LimitStatus = Literal["up", "down", "none"]


class AShareRecord(BaseModel):
    """Base model with shared A-share validation behavior."""

    model_config = ConfigDict(frozen=True)

    ts_code: TsCode

    @field_validator("ts_code")
    @classmethod
    def validate_ts_code(cls, value: str) -> str:
        """Ensure stock and index identifiers use Tushare exchange suffixes."""
        if not value or "." not in value:
            raise ValueError("ts_code must include an exchange suffix")

        symbol, suffix = value.rsplit(".", maxsplit=1)
        if not symbol.isdigit() or suffix not in {"SZ", "SH", "BJ"}:
            raise ValueError("ts_code must look like 000001.SZ, 600000.SH, or 430047.BJ")
        return value


class SecurityRecord(AShareRecord):
    """Security master data."""

    symbol: str
    name: str
    exchange: str
    market: str | None = None
    list_date: date | None = None
    delist_date: date | None = None
    is_active: bool = True


class TradeCalendarRecord(BaseModel):
    """Exchange trading calendar row."""

    model_config = ConfigDict(frozen=True)

    exchange: str
    cal_date: date
    is_open: bool
    pretrade_date: date | None = None


class PriceRangeMixin(BaseModel):
    """Shared OHLC validation."""

    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)

    @model_validator(mode="after")
    def validate_price_range(self) -> PriceRangeMixin:
        """Ensure OHLC prices form a consistent range."""
        if self.high < self.low:
            raise ValueError("high cannot be lower than low")
        if not self.low <= self.open <= self.high:
            raise ValueError("open must be between low and high")
        if not self.low <= self.close <= self.high:
            raise ValueError("close must be between low and high")
        return self


class DailyOHLCVRecord(AShareRecord, PriceRangeMixin):
    """Daily stock bar with A-share trading state flags."""

    trade_date: date
    pre_close: float | None = Field(default=None, gt=0)
    change: float | None = None
    pct_chg: float | None = None
    volume: float = Field(ge=0)
    amount: float = Field(ge=0)
    is_suspended: bool = False
    is_st: bool = False
    limit_status: LimitStatus = "none"


class AdjFactorRecord(AShareRecord):
    """Daily adjustment factor."""

    trade_date: date
    adj_factor: float = Field(gt=0)


class DailyBasicRecord(AShareRecord):
    """Tushare-style daily valuation and share data."""

    trade_date: date
    turnover_rate: float | None = Field(default=None, ge=0)
    volume_ratio: float | None = Field(default=None, ge=0)
    pe: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    ps: float | None = None
    ps_ttm: float | None = None
    total_share: float | None = Field(default=None, ge=0)
    float_share: float | None = Field(default=None, ge=0)
    free_share: float | None = Field(default=None, ge=0)
    total_mv: float | None = Field(default=None, ge=0)
    circ_mv: float | None = Field(default=None, ge=0)


class IndexDailyRecord(AShareRecord, PriceRangeMixin):
    """Daily index bar."""

    trade_date: date
    pre_close: float | None = Field(default=None, gt=0)
    change: float | None = None
    pct_chg: float | None = None
    volume: float = Field(ge=0)
    amount: float = Field(ge=0)


class FundamentalRecord(AShareRecord):
    """Point-in-time fundamental snapshot keyed by announcement date."""

    ann_date: date
    report_date: date
    report_type: str | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    ps_ttm: float | None = None
    roe: float | None = None
    roa: float | None = None
    gross_margin: float | None = None
    netprofit_yoy: float | None = None
    revenue_yoy: float | None = None
    debt_to_assets: float | None = None
    ocf_to_revenue: float | None = None


class FactorRecord(AShareRecord):
    """Computed factor value for a security and trading date."""

    trade_date: date
    factor_name: str
    factor_value: float
    factor_version: str = "default"


class ETLManifestRecord(BaseModel):
    """ETL load manifest row."""

    model_config = ConfigDict(frozen=True)

    dataset: str
    trade_date: date | None = None
    source: str
    version: str
    row_count: int = Field(ge=0)
    loaded_at: datetime
