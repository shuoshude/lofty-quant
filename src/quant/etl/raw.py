"""ETL raw CSV 读写和文件查找工具。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas import DataFrame

from quant.etl.etl_model import ETLTask
from quant.utils import (
    build_raw_path,
    is_daily_file_raw_dataset,
    is_single_file_raw_dataset,
    iter_raw_partition_dirs,
    parse_daily_raw_file_date,
)


def write_raw_csv(path: Path, df: DataFrame) -> int:
    """将 DataFrame 写入 raw CSV 并返回行数。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    return len(df.index)


def read_raw_csv(path: Path) -> DataFrame:
    """读取 raw CSV 为 DataFrame。"""
    return pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False)


def find_raw_files(raw_dir: Path, task: ETLTask, *, suffix: str = "csv") -> list[Path]:
    """查找任务范围内可能相关的 raw CSV 文件。"""
    if is_single_file_raw_dataset(task):
        path = build_raw_path(raw_dir, task, suffix=suffix)
        return [path] if path.is_file() else []

    files: list[Path] = []
    pattern = f"*.{suffix.lstrip('.')}"
    for partition_dir in iter_raw_partition_dirs(raw_dir, task):
        if partition_dir.exists():
            files.extend(partition_dir.glob(pattern))

    existing_files = sorted(path for path in files if path.is_file())
    if is_daily_file_raw_dataset(task):
        return [path for path in existing_files if _is_daily_raw_file_in_range(path, task)]
    return existing_files


def _is_daily_raw_file_in_range(path: Path, task: ETLTask) -> bool:
    """判断日频 raw 文件名中的交易日是否落在任务范围内。"""
    file_date = parse_daily_raw_file_date(path, task)
    return file_date is not None and task.start_date <= file_date <= task.end_date
