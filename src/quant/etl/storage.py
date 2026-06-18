"""ETL 目标存储写入工具。"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
from duckdb import DuckDBPyConnection
from pydantic import BaseModel

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TEMP_DATAFRAME_VIEW = "__etl_df"


def write_processed_parquet(
    processed_dir: Path,
    *,
    dataset: str,
    partition_date: date,
    records: Iterable[Mapping[str, Any] | BaseModel],
    filename: str | None = None,
) -> Path:
    """按 year/month 分区写入处理后 Parquet。"""
    rows = [_record_to_dict(record) for record in records]
    output_dir = (
        processed_dir.expanduser().resolve()
        / dataset
        / f"year={partition_date:%Y}"
        / f"month={partition_date:%m}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / (filename or f"{dataset}_{partition_date:%Y%m%d}.parquet")
    pl.DataFrame(rows).write_parquet(output_path)
    return output_path


def insert_duckdb_records(
    conn: DuckDBPyConnection,
    *,
    table: str,
    records: Iterable[Mapping[str, Any] | BaseModel],
    columns: Sequence[str],
) -> int:
    """向 DuckDB 表批量插入记录。"""
    _validate_identifier(table)
    for column in columns:
        _validate_identifier(column)

    rows = [_record_to_dict(record) for record in records]
    if not rows:
        return 0

    placeholders = ", ".join("?" for _ in columns)
    column_sql = ", ".join(columns)
    values = [tuple(row.get(column) for column in columns) for row in rows]
    conn.executemany(
        f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})",
        values,
    )
    return len(values)


def replace_duckdb_records(
    conn: DuckDBPyConnection,
    *,
    table: str,
    records: Iterable[Mapping[str, Any] | BaseModel],
    columns: Sequence[str],
    delete_where: str,
    delete_params: Sequence[Any],
) -> int:
    """先删除目标范围再批量插入记录。"""
    _validate_identifier(table)
    conn.execute(f"DELETE FROM {table} WHERE {delete_where}", list(delete_params))
    return insert_duckdb_records(conn, table=table, records=records, columns=columns)


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


def _record_to_dict(record: Mapping[str, Any] | BaseModel) -> dict[str, Any]:
    """将映射或 Pydantic 模型转换为字典。"""
    if isinstance(record, BaseModel):
        return record.model_dump()
    return dict(record)


def _validate_identifier(value: str) -> None:
    """校验 SQL 标识符, 避免动态 SQL 注入。"""
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"无效的 SQL 标识符: {value}")
