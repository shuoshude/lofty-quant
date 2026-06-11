"""Repository layer: the only public SQL query interface for research data."""

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
    """Read-only query facade over DuckDB tables and registered views."""

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
        """Return daily bars ordered by security and trading date."""
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
        """Return a daily stock cross section with caller-selected columns."""
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
        """Return factor values for a date and one or more factor names."""
        if not factor_names:
            raise ValueError("factor_names cannot be empty")

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
        """Return trading calendar rows ordered by calendar date."""
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
        """Execute a query and return rows as dictionaries."""
        result = self._conn.execute(query, params)
        columns = [column[0] for column in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def _validate_fields(fields: Sequence[str]) -> list[str]:
    """Validate SQL identifiers used for selected cross-section columns."""
    if not fields:
        raise ValueError("fields cannot be empty")

    invalid = [field for field in fields if not IDENTIFIER_PATTERN.fullmatch(field)]
    if invalid:
        raise ValueError(f"invalid field names: {invalid}")
    return list(fields)
