"""Repository 层: 研究数据唯一公开 SQL 查询入口。"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import date
from typing import Any

from duckdb import DuckDBPyConnection

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
        adjusted: bool = True,
    ) -> list[dict[str, Any]]:
        """按证券和交易日排序返回日线行情。"""
        source = "v_daily_adj" if adjusted else "v_daily_ohlcv"
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
