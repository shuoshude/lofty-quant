"""ETL raw 加载和目标存储写入工具。"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from duckdb import DuckDBPyConnection
from pydantic import BaseModel

from quant.etl.fetch import ETLTask

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_raw_data(task: ETLTask) -> int:
    """执行 raw 到目标存储加载。

    当前还没有接入真实数据集, 这里保留统一入口并给出明确错误。
    """
    raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")


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


def write_manifest(
    conn: DuckDBPyConnection,
    *,
    dataset: str,
    trade_date: date | None,
    source: str,
    version: str,
    row_count: int,
    loaded_at: datetime | None = None,
) -> None:
    """写入或覆盖 ETL 加载清单。"""
    resolved_loaded_at = loaded_at or datetime.now()
    conn.execute(
        """
        DELETE FROM etl_manifest
        WHERE dataset = ?
          AND source = ?
          AND version = ?
          AND (trade_date = ? OR (trade_date IS NULL AND ? IS NULL))
        """,
        [dataset, source, version, trade_date, trade_date],
    )
    conn.execute(
        """
        INSERT INTO etl_manifest (dataset, trade_date, source, version, row_count, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [dataset, trade_date, source, version, row_count, resolved_loaded_at],
    )


def get_manifest_status(
    conn: DuckDBPyConnection,
    *,
    dataset: str,
    source: str | None = None,
) -> dict[str, Any]:
    """读取简单加载状态。"""
    source_filter = ""
    params: list[Any] = [dataset]
    if source is not None:
        source_filter = "AND source = ?"
        params.append(source)

    row = conn.execute(
        f"""
        SELECT COUNT(*), MAX(trade_date), MAX(loaded_at)
        FROM etl_manifest
        WHERE dataset = ?
        {source_filter}
        """,
        params,
    ).fetchone()
    if row is None:
        return {"loaded_count": 0, "latest_trade_date": None, "latest_loaded_at": None}

    loaded_count, latest_trade_date, latest_loaded_at = row
    return {
        "loaded_count": int(loaded_count),
        "latest_trade_date": latest_trade_date,
        "latest_loaded_at": latest_loaded_at,
    }


def _record_to_dict(record: Mapping[str, Any] | BaseModel) -> dict[str, Any]:
    """将映射或 Pydantic 模型转换为字典。"""
    if isinstance(record, BaseModel):
        return record.model_dump()
    return dict(record)


def _validate_identifier(value: str) -> None:
    """校验 SQL 标识符, 避免动态 SQL 注入。"""
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"无效的 SQL 标识符: {value}")
