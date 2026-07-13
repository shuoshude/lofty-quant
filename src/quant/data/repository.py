"""Repository 层: 研究数据唯一公开 SQL 查询入口。"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import date
from typing import Any, Literal, cast

import polars as pl
from duckdb import DuckDBPyConnection

from quant.data.fields import DAILY_OHLCV_COLUMNS

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_CROSS_SECTION_FIELDS = (
    "ts_code",
    "trade_date",
    "close",
    "volume",
    "amount",
    "is_suspended",
    "is_st",
    "limit_status",
)
AdjustmentMode = Literal["none", "hfq", "qfq"]
PanelAdjustmentMode = Literal["none", "hfq"]
HFQ_DAILY_PANEL_FIELDS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "cumulative_factor",
    "hfq_open",
    "hfq_high",
    "hfq_low",
    "hfq_close",
    "volume",
    "amount",
    "is_suspended",
    "is_st",
    "limit_status",
)


class QuantRepository:
    """面向 DuckDB 表和已注册视图的只读查询门面。"""

    def __init__(self, conn: DuckDBPyConnection) -> None:
        self._conn = conn

    def get_daily_bars(
        self,
        ts_code: str,
        start: date,
        end: date,
        *,
        adjustment: AdjustmentMode = "none",
        as_of_date: date | None = None,
        adjusted: bool | None = None,
    ) -> list[dict[str, Any]]:
        """按证券和交易日排序返回日线行情。"""
        if adjusted is not None:
            adjustment = "qfq" if adjusted else "none"

        if adjustment == "qfq":
            return self._get_qfq_daily_bars(ts_code, start, end, as_of_date=as_of_date)

        source = _daily_bar_source(adjustment)
        return self._fetch_dicts(
            f"""
            SELECT *
            FROM {source}
            WHERE ts_code = ?
              AND trade_date BETWEEN ? AND ?
            ORDER BY ts_code, trade_date
            """,
            [ts_code, start, end],
        )

    def get_daily_panel(
        self,
        start: date,
        end: date,
        fields: Sequence[str],
        *,
        adjustment: PanelAdjustmentMode = "hfq",
    ) -> pl.DataFrame:
        """返回全市场指定日期区间的日频研究面板。"""
        source = _daily_panel_source(adjustment)
        requested_fields = _validate_daily_panel_fields(fields, adjustment)
        selected_fields = list(dict.fromkeys(["ts_code", "trade_date", *requested_fields]))
        result = self._conn.execute(
            f"""
            SELECT {", ".join(selected_fields)}
            FROM {source}
            WHERE trade_date BETWEEN ? AND ?
            ORDER BY ts_code, trade_date
            """,
            [start, end],
        )
        return result.pl()

    def _get_qfq_daily_bars(
        self,
        ts_code: str,
        start: date,
        end: date,
        *,
        as_of_date: date | None,
    ) -> list[dict[str, Any]]:
        """按 as-of 因子计算前复权日线, 避免使用未来因子。"""
        effective_as_of = as_of_date or end
        effective_end = min(end, effective_as_of)
        if effective_end < start:
            return []

        return self._fetch_dicts(
            """
            WITH base AS (
                SELECT *
                FROM v_daily_ohlcv
                WHERE ts_code = ?
                  AND trade_date BETWEEN ? AND ?
            ),
            asof_factor AS (
                SELECT cumulative_factor AS asof_cumulative_factor
                FROM v_adj_factor
                WHERE ts_code = ?
                  AND trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT 1
            )
            SELECT
                b.ts_code,
                b.trade_date,
                b.open,
                b.high,
                b.low,
                b.close,
                f.cumulative_factor,
                af.asof_cumulative_factor,
                b.open * f.cumulative_factor / af.asof_cumulative_factor AS qfq_open,
                b.high * f.cumulative_factor / af.asof_cumulative_factor AS qfq_high,
                b.low * f.cumulative_factor / af.asof_cumulative_factor AS qfq_low,
                b.close * f.cumulative_factor / af.asof_cumulative_factor AS qfq_close,
                b.volume,
                b.amount,
                b.is_suspended,
                b.is_st,
                b.limit_status
            FROM base b
            LEFT JOIN v_adj_factor f
              ON b.ts_code = f.ts_code
             AND b.trade_date = f.trade_date
            LEFT JOIN asof_factor af
              ON TRUE
            ORDER BY b.ts_code, b.trade_date
            """,
            [ts_code, start, effective_end, ts_code, effective_as_of],
        )

    def get_cross_section(
        self,
        trade_date: date,
        fields: Sequence[str] = DEFAULT_CROSS_SECTION_FIELDS,
        *,
        exclude_suspended: bool = False,
    ) -> list[dict[str, Any]]:
        """按调用方选择的字段返回日度股票截面。"""
        selected_fields = ", ".join(_validate_fields(fields))
        suspended_filter = "AND is_suspended = FALSE" if exclude_suspended else ""
        return self._fetch_dicts(
            f"""
            SELECT {selected_fields}
            FROM v_daily_ohlcv
            WHERE trade_date = ?
            {suspended_filter}
            ORDER BY ts_code
            """,
            [trade_date],
        )

    def get_factors(
        self,
        trade_date: date,
        factor_names: Sequence[str],
        *,
        factor_version: str | None = None,
    ) -> list[dict[str, Any]]:
        """返回某交易日一个或多个因子的取值。"""
        if not factor_names:
            raise ValueError("factor_names 不能为空")

        placeholders = ", ".join("?" for _ in factor_names)
        params: list[Any] = [trade_date, *factor_names]
        version_filter = ""
        if factor_version is not None:
            version_filter = "AND factor_version = ?"
            params.append(factor_version)

        return self._fetch_dicts(
            f"""
            SELECT ts_code, trade_date, factor_name, factor_value, factor_version
            FROM v_factors
            WHERE trade_date = ?
              AND factor_name IN ({placeholders})
              {version_filter}
            ORDER BY ts_code, factor_name
            """,
            params,
        )

    def get_trade_calendar(
        self,
        start: date,
        end: date,
        *,
        exchange: str = "SSE",
    ) -> list[dict[str, Any]]:
        """按日历日期排序返回交易日历记录。"""
        return self._fetch_dicts(
            """
            SELECT exchange, cal_date, is_open, pretrade_date
            FROM dim_trade_calendar
            WHERE exchange = ?
              AND cal_date BETWEEN ? AND ?
            ORDER BY cal_date
            """,
            [exchange, start, end],
        )

    def get_open_trade_dates(
        self,
        start: date,
        end: date,
        *,
        exchange: str = "SSE",
    ) -> list[date]:
        """按日期排序返回指定交易所开市日。"""
        rows = self._conn.execute(
            """
            SELECT cal_date
            FROM dim_trade_calendar
            WHERE exchange = ?
              AND is_open = TRUE
              AND cal_date BETWEEN ? AND ?
            ORDER BY cal_date
            """,
            [exchange, start, end],
        ).fetchall()
        return [cast(date, row[0]) for row in rows]

    def _fetch_dicts(self, query: str, params: Sequence[Any]) -> list[dict[str, Any]]:
        """执行查询并以字典列表返回结果。"""
        result = self._conn.execute(query, params)
        columns = [column[0] for column in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def _validate_fields(fields: Sequence[str]) -> list[str]:
    """校验截面字段中的 SQL 标识符。"""
    if not fields:
        raise ValueError("fields 不能为空")

    invalid = [field for field in fields if not IDENTIFIER_PATTERN.fullmatch(field)]
    if invalid:
        raise ValueError(f"无效的字段名: {invalid}")
    return list(fields)


def _daily_bar_source(adjustment: AdjustmentMode) -> str:
    """返回复权模式对应的 DuckDB 视图。"""
    if adjustment == "none":
        return "v_daily_ohlcv"
    if adjustment == "hfq":
        return "v_daily_hfq"
    if adjustment == "qfq":
        return "v_daily_qfq_latest"
    raise ValueError(f"不支持的复权模式: {adjustment}")


def _daily_panel_source(adjustment: PanelAdjustmentMode) -> str:
    """返回研究面板复权模式对应的 DuckDB 视图。"""
    if adjustment == "none":
        return "v_daily_ohlcv"
    if adjustment == "hfq":
        return "v_daily_hfq"
    raise ValueError(f"研究面板不支持的复权模式: {adjustment}")


def _validate_daily_panel_fields(
    fields: Sequence[str],
    adjustment: PanelAdjustmentMode,
) -> list[str]:
    """校验研究面板字段在指定复权视图中可用。"""
    validated_fields = _validate_fields(fields)
    available_fields = DAILY_OHLCV_COLUMNS if adjustment == "none" else HFQ_DAILY_PANEL_FIELDS
    unavailable_fields = [field for field in validated_fields if field not in available_fields]
    if unavailable_fields:
        raise ValueError(f"研究面板字段不适用于 adjustment={adjustment}: {unavailable_fields}")
    return validated_fields
