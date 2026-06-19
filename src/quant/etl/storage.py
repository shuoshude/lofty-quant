"""ETL 目标存储写入工具。"""

from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
from duckdb import DuckDBPyConnection

from quant.data.db import DuckDBManager

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TEMP_DATAFRAME_VIEW = "__etl_df"


def replace_table_dataframe(
    database_path: Path,
    processed_dir: Path,
    *,
    table: str,
    df: pd.DataFrame,
    columns: Sequence[str],
    delete_where: str,
    delete_params: Sequence[Any],
) -> int:
    """初始化 DuckDB 后, 用 DataFrame 覆盖目标表范围。"""
    manager = DuckDBManager(database_path, processed_dir)
    manager.initialize()
    with manager.session() as conn:
        return replace_duckdb_dataframe(
            conn,
            table=table,
            df=df,
            columns=columns,
            delete_where=delete_where,
            delete_params=delete_params,
        )


def replace_duckdb_dataframe(
    conn: DuckDBPyConnection,
    *,
    table: str,
    df: pd.DataFrame,
    columns: Sequence[str],
    delete_where: str,
    delete_params: Sequence[Any],
) -> int:
    """先删除目标范围, 再通过 DataFrame 临时视图批量写入 DuckDB。"""
    _validate_identifier(table)
    for column in columns:
        _validate_identifier(column)

    conn.execute(f"DELETE FROM {table} WHERE {delete_where}", list(delete_params))
    if df.empty:
        return 0

    selected_df = df.loc[:, list(columns)]
    column_sql = ", ".join(columns)
    try:
        conn.register(TEMP_DATAFRAME_VIEW, selected_df)
        conn.execute(
            f"""
            INSERT INTO {table} ({column_sql})
            SELECT {column_sql}
            FROM {TEMP_DATAFRAME_VIEW}
            """
        )
    finally:
        conn.unregister(TEMP_DATAFRAME_VIEW)
    return len(selected_df.index)


def _validate_identifier(value: str) -> None:
    """校验 SQL 标识符, 避免动态 SQL 注入。"""
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"无效的 SQL 标识符: {value}")
