"""A 股数据集的数据契约和校验规则。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from quant.data.fields import (
    ADJ_FACTOR_FIELD_COMMENTS,
    DAILY_BASIC_FIELD_COMMENTS,
    DAILY_OHLCV_FIELD_COMMENTS,
)

TsCode = str
LimitStatus = Literal["up", "down", "none"]


class AShareRecord(BaseModel):
    """带有 A 股通用校验逻辑的基础模型。"""

    model_config = ConfigDict(frozen=True)

    ts_code: TsCode = Field(description=DAILY_OHLCV_FIELD_COMMENTS["ts_code"])

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

    symbol: str = Field(description="证券数字代码")
    name: str = Field(description="证券简称")
    exchange: str = Field(description="交易所代码")
    market: str | None = Field(default=None, description="市场板块")
    list_date: date | None = Field(default=None, description="上市日期")
    delist_date: date | None = Field(default=None, description="退市日期")
    is_active: bool = Field(default=True, description="是否仍处于活跃交易状态")


class TradeCalendarRecord(BaseModel):
    """交易所交易日历记录。"""

    model_config = ConfigDict(frozen=True)

    exchange: str = Field(description="交易所代码")
    cal_date: date = Field(description="自然日")
    is_open: bool = Field(description="是否开市")
    pretrade_date: date | None = Field(default=None, description="上一交易日")


class PriceRangeMixin(BaseModel):
    """OHLC 价格区间通用校验。"""

    open: float = Field(gt=0, description=DAILY_OHLCV_FIELD_COMMENTS["open"])
    high: float = Field(gt=0, description=DAILY_OHLCV_FIELD_COMMENTS["high"])
    low: float = Field(gt=0, description=DAILY_OHLCV_FIELD_COMMENTS["low"])
    close: float = Field(gt=0, description=DAILY_OHLCV_FIELD_COMMENTS["close"])

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

    trade_date: date = Field(description=DAILY_OHLCV_FIELD_COMMENTS["trade_date"])
    pre_close: float | None = Field(
        default=None,
        description=DAILY_OHLCV_FIELD_COMMENTS["pre_close"],
    )
    change: float | None = Field(default=None, description=DAILY_OHLCV_FIELD_COMMENTS["change"])
    pct_chg: float | None = Field(default=None, description=DAILY_OHLCV_FIELD_COMMENTS["pct_chg"])
    volume: float = Field(ge=0, description=DAILY_OHLCV_FIELD_COMMENTS["volume"])
    amount: float = Field(ge=0, description=DAILY_OHLCV_FIELD_COMMENTS["amount"])
    is_suspended: bool = Field(
        default=False,
        description=DAILY_OHLCV_FIELD_COMMENTS["is_suspended"],
    )
    is_st: bool = Field(default=False, description=DAILY_OHLCV_FIELD_COMMENTS["is_st"])
    limit_status: LimitStatus = Field(
        default="none",
        description=DAILY_OHLCV_FIELD_COMMENTS["limit_status"],
    )


class AdjFactorRecord(AShareRecord):
    """每日复权因子。"""

    trade_date: date = Field(description=ADJ_FACTOR_FIELD_COMMENTS["trade_date"])
    cumulative_factor: float = Field(
        gt=0,
        description=ADJ_FACTOR_FIELD_COMMENTS["cumulative_factor"],
    )


class DailyBasicRecord(AShareRecord):
    """Tushare 风格的每日估值和股本数据。"""

    trade_date: date = Field(description=DAILY_BASIC_FIELD_COMMENTS["trade_date"])
    close: float | None = Field(default=None, ge=0, description=DAILY_BASIC_FIELD_COMMENTS["close"])
    turnover_rate: float | None = Field(
        default=None,
        ge=0,
        description=DAILY_BASIC_FIELD_COMMENTS["turnover_rate"],
    )
    turnover_rate_f: float | None = Field(
        default=None,
        ge=0,
        description=DAILY_BASIC_FIELD_COMMENTS["turnover_rate_f"],
    )
    volume_ratio: float | None = Field(
        default=None,
        ge=0,
        description=DAILY_BASIC_FIELD_COMMENTS["volume_ratio"],
    )
    pe: float | None = Field(default=None, description=DAILY_BASIC_FIELD_COMMENTS["pe"])
    pe_ttm: float | None = Field(default=None, description=DAILY_BASIC_FIELD_COMMENTS["pe_ttm"])
    pb: float | None = Field(default=None, description=DAILY_BASIC_FIELD_COMMENTS["pb"])
    ps: float | None = Field(default=None, description=DAILY_BASIC_FIELD_COMMENTS["ps"])
    ps_ttm: float | None = Field(default=None, description=DAILY_BASIC_FIELD_COMMENTS["ps_ttm"])
    dv_ratio: float | None = Field(
        default=None,
        ge=0,
        description=DAILY_BASIC_FIELD_COMMENTS["dv_ratio"],
    )
    dv_ttm: float | None = Field(
        default=None,
        ge=0,
        description=DAILY_BASIC_FIELD_COMMENTS["dv_ttm"],
    )
    total_share: float | None = Field(
        default=None,
        ge=0,
        description=DAILY_BASIC_FIELD_COMMENTS["total_share"],
    )
    float_share: float | None = Field(
        default=None,
        ge=0,
        description=DAILY_BASIC_FIELD_COMMENTS["float_share"],
    )
    free_share: float | None = Field(
        default=None,
        ge=0,
        description=DAILY_BASIC_FIELD_COMMENTS["free_share"],
    )
    total_mv: float | None = Field(
        default=None,
        ge=0,
        description=DAILY_BASIC_FIELD_COMMENTS["total_mv"],
    )
    circ_mv: float | None = Field(
        default=None,
        ge=0,
        description=DAILY_BASIC_FIELD_COMMENTS["circ_mv"],
    )


class IndexDailyRecord(AShareRecord, PriceRangeMixin):
    """指数日线行情。"""

    trade_date: date = Field(description="交易日")
    pre_close: float | None = Field(default=None, gt=0, description="前收盘点位")
    change: float | None = Field(default=None, description="涨跌额")
    pct_chg: float | None = Field(default=None, description="涨跌幅, 单位为百分比")
    volume: float = Field(ge=0, description="成交量")
    amount: float = Field(ge=0, description="成交额")


class FundamentalRecord(AShareRecord):
    """按公告日期归档的基本面时点快照。"""

    ann_date: date = Field(description="公告日期")
    report_date: date = Field(description="报告期")
    report_type: str | None = Field(default=None, description="报告类型")
    pe_ttm: float | None = Field(default=None, description="滚动市盈率")
    pb: float | None = Field(default=None, description="市净率")
    ps_ttm: float | None = Field(default=None, description="滚动市销率")
    roe: float | None = Field(default=None, description="净资产收益率")
    roa: float | None = Field(default=None, description="总资产收益率")
    gross_margin: float | None = Field(default=None, description="毛利率")
    netprofit_yoy: float | None = Field(default=None, description="净利润同比增速")
    revenue_yoy: float | None = Field(default=None, description="营业收入同比增速")
    debt_to_assets: float | None = Field(default=None, description="资产负债率")
    ocf_to_revenue: float | None = Field(default=None, description="经营现金流与收入比")


class FactorRecord(AShareRecord):
    """某证券在交易日上的因子计算结果。"""

    trade_date: date = Field(description="交易日")
    factor_name: str = Field(description="因子名称")
    factor_value: float = Field(description="因子值")
    factor_version: str = Field(default="default", description="因子版本")


class ETLManifestRecord(BaseModel):
    """ETL 加载清单记录。"""

    model_config = ConfigDict(frozen=True)

    dataset: str = Field(description="数据集名称")
    trade_date: date | None = Field(default=None, description="交易日")
    source: str = Field(description="数据源名称")
    version: str = Field(description="数据版本")
    row_count: int = Field(ge=0, description="加载行数")
    loaded_at: datetime = Field(description="加载完成时间")
