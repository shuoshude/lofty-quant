"""ETL raw 加载和目标存储写入工具。"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from math import isnan
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
from duckdb import DuckDBPyConnection
from pydantic import BaseModel

from quant.config import QuantConfig
from quant.data.db import DuckDBManager
from quant.data.schemas import TradeCalendarRecord
from quant.etl.etl_model import ETLTask
from quant.etl.fetch import find_raw_files, read_raw_csv

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_raw_data(config: QuantConfig, task: ETLTask) -> int:
    """执行 raw CSV 到目标存储加载。"""
    if task.dataset == "trade-calendar":
        return load_trade_calendar(config, task)
    raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")


def load_trade_calendar(config: QuantConfig, task: ETLTask) -> int:
    """读取交易日历 raw CSV, 校验后写入 DuckDB 维表。"""
    raw_files = find_raw_files(config.paths.raw_dir, task)
    if not raw_files:
        raise FileNotFoundError(
            f"未找到 raw CSV 文件: dataset={task.dataset}, source={task.source}"
        )

    raw_df = _read_csv_files(raw_files)
    records = _normalize_trade_calendar_records(raw_df, task)

    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
        if task.exchange:
            delete_where = "exchange = ? AND cal_date BETWEEN ? AND ?"
            delete_params: Sequence[Any] = [task.exchange.upper(), task.start_date, task.end_date]
        else:
            delete_where = "cal_date BETWEEN ? AND ?"
            delete_params = [task.start_date, task.end_date]

        row_count = replace_duckdb_records(
            conn,
            table="dim_trade_calendar",
            records=records,
            columns=["exchange", "cal_date", "is_open", "pretrade_date"],
            delete_where=delete_where,
            delete_params=delete_params,
        )
        write_manifest(
            conn,
            dataset=task.dataset,
            trade_date=task.end_date,
            source=task.source,
            version="default",
            row_count=row_count,
        )
    return row_count


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


def _read_csv_files(paths: Sequence[Path]) -> pd.DataFrame:
    """读取一个或多个 raw CSV 文件。"""
    frames = [read_raw_csv(path) for path in paths]
    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames, ignore_index=True)


def _normalize_trade_calendar_records(df: pd.DataFrame, task: ETLTask) -> list[TradeCalendarRecord]:
    """将交易日历 raw DataFrame 转为项目数据契约。"""
    records: list[TradeCalendarRecord] = []
    for row in df.to_dict(orient="records"):
        cal_date = _parse_required_date(row.get("cal_date"), field_name="cal_date")
        pretrade_date = _parse_optional_date(row.get("pretrade_date"), field_name="pretrade_date")
        record = TradeCalendarRecord(
            exchange=_normalize_exchange(row.get("exchange"), task),
            cal_date=cal_date,
            is_open=_parse_bool(row.get("is_open"), field_name="is_open"),
            pretrade_date=pretrade_date,
        )
        records.append(record)
    return records


def _normalize_exchange(value: object, task: ETLTask) -> str:
    """标准化交易所代码。"""
    if value is not None and not _is_missing(value):
        exchange = str(value).strip()
    else:
        exchange = task.exchange or "SSE"
    return exchange.upper()


def _parse_required_date(value: object, *, field_name: str) -> date:
    """解析必填日期字段。"""
    parsed = _parse_optional_date(value, field_name=field_name)
    if parsed is None:
        raise ValueError(f"{field_name} 不能为空")
    return parsed


def _parse_optional_date(value: object, *, field_name: str) -> date | None:
    """解析可选日期字段。"""
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(int(value)) if isinstance(value, int | float) else str(value).strip()
    if not text:
        return None

    for pattern in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"日期字段 {field_name} 格式无效: {value}")


def _parse_bool(value: object, *, field_name: str) -> bool:
    """解析布尔字段。"""
    if _is_missing(value):
        raise ValueError(f"{field_name} 不能为空")
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return int(value) == 1

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "open", "开市", "是"}:
        return True
    if text in {"0", "false", "no", "n", "close", "closed", "休市", "否"}:
        return False
    raise ValueError(f"布尔字段 {field_name} 格式无效: {value}")


def _is_missing(value: object) -> bool:
    """判断 DataFrame 单元格是否为空。"""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"", "nan", "nat", "none", "<na>"}
    if isinstance(value, float):
        return isnan(value)
    return False
