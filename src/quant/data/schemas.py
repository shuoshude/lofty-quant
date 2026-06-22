"""A 股数据集的数据契约和校验规则。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

TsCode = str
LimitStatus = Literal["up", "down", "none"]


class AShareRecord(BaseModel):
    """带有 A 股通用校验逻辑的基础模型。"""

    model_config = ConfigDict(frozen=True)

    ts_code: TsCode

    @field_validator("ts_code")
    @classmethod
    def validate_ts_code(cls, value: str) -> str:
        """校验股票和指数代码是否使用 Tushare 交易所后缀。"""
        if not value or "." not in value:
            raise ValueError("ts_code 必须包含交易所后缀")

        symbol, suffix = value.rsplit(".", maxsplit=1)
        if not symbol.isdigit() or suffix not in {"SZ", "SH", "BJ"}:
            raise ValueError("ts_code 格式应类似 000001.SZ, 600000.SH 或 430047.BJ")
        return value


class SecurityRecord(AShareRecord):
    """证券主数据。"""

    symbol: str
    name: str
    exchange: str
    market: str | None = None
    list_date: date | None = None
    delist_date: date | None = None
    is_active: bool = True


class TradeCalendarRecord(BaseModel):
    """交易所交易日历记录。"""

    model_config = ConfigDict(frozen=True)

    exchange: str
    cal_date: date
    is_open: bool
    pretrade_date: date | None = None


class PriceRangeMixin(BaseModel):
    """OHLC 价格区间通用校验。"""

    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)

    @model_validator(mode="after")
    def validate_price_range(self) -> PriceRangeMixin:
        """校验 OHLC 价格是否构成一致区间。"""
        if self.high < self.low:
            raise ValueError("high 不能低于 low")
        if not self.low <= self.open <= self.high:
            raise ValueError("open 必须位于 low 和 high 之间")
        if not self.low <= self.close <= self.high:
            raise ValueError("close 必须位于 low 和 high 之间")
        return self


class DailyOHLCVRecord(AShareRecord, PriceRangeMixin):
    """包含 A 股交易状态标记的股票日线行情。"""

    trade_date: date
    pre_close: float | None = Field(default=None)
    change: float | None = None
    pct_chg: float | None = None
    volume: float = Field(ge=0)
    amount: float = Field(ge=0)
    is_suspended: bool = False
    is_st: bool = False
    limit_status: LimitStatus = "none"


class AdjFactorRecord(AShareRecord):
    """每日复权因子。"""

    trade_date: date
    cumulative_factor: float = Field(gt=0)


class DailyBasicRecord(AShareRecord):
    """Tushare 风格的每日估值和股本数据。"""

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
    """指数日线行情。"""

    trade_date: date
    pre_close: float | None = Field(default=None, gt=0)
    change: float | None = None
    pct_chg: float | None = None
    volume: float = Field(ge=0)
    amount: float = Field(ge=0)


class FundamentalRecord(AShareRecord):
    """按公告日期归档的基本面时点快照。"""

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
    """某证券在交易日上的因子计算结果。"""

    trade_date: date
    factor_name: str
    factor_value: float
    factor_version: str = "default"


class ETLManifestRecord(BaseModel):
    """ETL 加载清单记录。"""

    model_config = ConfigDict(frozen=True)

    dataset: str
    trade_date: date | None = None
    source: str
    version: str
    row_count: int = Field(ge=0)
    loaded_at: datetime
