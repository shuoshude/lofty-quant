"""A 股数据集的数据契约和校验规则。"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from quant.data.fields import (
    ADJ_FACTOR_FIELD_COMMENTS,
    DAILY_BASIC_FIELD_COMMENTS,
    DAILY_OHLCV_FIELD_COMMENTS,
    SECURITY_FIELD_COMMENTS,
)

TsCode = str
LimitStatus = Literal[-1, 0, 1, 2, 3, 4]
SecurityListStatus = Literal["L", "D", "P"]


def _is_missing_float(value: float | None) -> bool:
    """判断可选浮点字段是否缺失。"""
    return value is None or (isinstance(value, float) and math.isnan(value))


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

    symbol: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["symbol"])
    name: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["name"])
    area: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["area"])
    industry: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["industry"])
    fullname: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["fullname"])
    enname: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["enname"])
    cnspell: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["cnspell"])
    market: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["market"])
    exchange: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["exchange"])
    curr_type: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["curr_type"])
    list_status: SecurityListStatus | None = Field(
        default=None,
        description=SECURITY_FIELD_COMMENTS["list_status"],
    )
    list_date: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["list_date"])
    delist_date: str | None = Field(
        default=None,
        description=SECURITY_FIELD_COMMENTS["delist_date"],
    )
    is_hs: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["is_hs"])
    act_name: str | None = Field(default=None, description=SECURITY_FIELD_COMMENTS["act_name"])
    act_ent_type: str | None = Field(
        default=None,
        description=SECURITY_FIELD_COMMENTS["act_ent_type"],
    )


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


class DailyOHLCVRecord(AShareRecord):
    """包含 A 股交易状态标记的股票日线行情。"""

    trade_date: date = Field(description=DAILY_OHLCV_FIELD_COMMENTS["trade_date"])
    open: float | None = Field(default=None, description=DAILY_OHLCV_FIELD_COMMENTS["open"])
    high: float | None = Field(default=None, description=DAILY_OHLCV_FIELD_COMMENTS["high"])
    low: float | None = Field(default=None, description=DAILY_OHLCV_FIELD_COMMENTS["low"])
    close: float | None = Field(default=None, description=DAILY_OHLCV_FIELD_COMMENTS["close"])
    pre_close: float | None = Field(
        default=None,
        description=DAILY_OHLCV_FIELD_COMMENTS["pre_close"],
    )
    change: float | None = Field(default=None, description=DAILY_OHLCV_FIELD_COMMENTS["change"])
    pct_chg: float | None = Field(default=None, description=DAILY_OHLCV_FIELD_COMMENTS["pct_chg"])
    volume: float | None = Field(default=None, description=DAILY_OHLCV_FIELD_COMMENTS["volume"])
    amount: float | None = Field(default=None, description=DAILY_OHLCV_FIELD_COMMENTS["amount"])
    is_suspended: bool = Field(
        default=False,
        description=DAILY_OHLCV_FIELD_COMMENTS["is_suspended"],
    )
    is_st: bool = Field(default=False, description=DAILY_OHLCV_FIELD_COMMENTS["is_st"])
    limit_status: LimitStatus = Field(
        default=0,
        description=DAILY_OHLCV_FIELD_COMMENTS["limit_status"],
    )

    @model_validator(mode="after")
    def validate_trading_state(self) -> DailyOHLCVRecord:
        """按停牌状态校验行情字段和涨跌停状态。"""
        if self.is_suspended:
            if self.limit_status != -1:
                raise ValueError("全天停牌行 limit_status 必须为 -1")
            return self

        if self.limit_status == -1:
            raise ValueError("非停牌行 limit_status 不能为 -1")

        required_fields = {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
        }
        missing_fields = [
            field_name for field_name, value in required_fields.items() if _is_missing_float(value)
        ]
        if missing_fields:
            raise ValueError(f"非停牌行行情字段不能为空: {missing_fields}")

        assert self.open is not None
        assert self.high is not None
        assert self.low is not None
        assert self.close is not None
        assert self.volume is not None
        assert self.amount is not None

        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("非停牌行 OHLC 价格必须大于 0")
        if self.volume < 0 or self.amount < 0:
            raise ValueError("非停牌行成交量和成交额不能小于 0")
        if self.high < self.low:
            raise ValueError("high 不能低于 low")
        if not self.low <= self.open <= self.high:
            raise ValueError("open 必须位于 low 和 high 之间")
        if not self.low <= self.close <= self.high:
            raise ValueError("close 必须位于 low 和 high 之间")
        return self


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
