"""ETL 原始数据拉取和 raw 落盘工具。"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import date
from pathlib import Path
from typing import Any
from .sources.tushare_source import TushareClient
from .etl_model import ETLTask


RawRecord = Mapping[str, Any]


def build_raw_path(raw_dir: Path, task: ETLTask, *, suffix: str = "csv") -> Path:
    """生成 raw 文件路径。"""
    normalized_suffix = suffix.lstrip(".")
    partition_dir = (
        raw_dir.expanduser().resolve()
        / task.source
        / task.dataset
        / f"year={task.start_date:%Y}"
        / f"month={task.start_date:%m}"
    )
    filename = (
        f"{task.dataset}_{task.source}_{task.start_date:%Y%m%d}_"
        f"{task.end_date:%Y%m%d}.{normalized_suffix}"
    )
    return partition_dir / filename


def write_jsonl(path: Path, records: Iterable[RawRecord]) -> int:
    """将原始记录写入 JSONL 文件并返回行数。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(dict(record), ensure_ascii=False, default=str))
            file.write("\n")
            row_count += 1
    return row_count


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """读取 JSONL 原始记录。"""
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            content = line.strip()
            if not content:
                continue
            loaded = json.loads(content)
            if not isinstance(loaded, dict):
                raise ValueError(f"JSONL 第 {line_number} 行不是对象: {path}")
            records.append(loaded)
    return records


def find_raw_files(raw_dir: Path, task: ETLTask, *, suffix: str = "csv") -> list[Path]:
    """按年月分区查找日期范围内可能相关的 raw 文件。"""
    files: list[Path] = []
    pattern = f"*.{suffix.lstrip('.')}"
    for partition_date in _iter_months(task.start_date, task.end_date):
        partition_dir = (
            raw_dir.expanduser().resolve()
            / task.source
            / task.dataset
            / f"year={partition_date:%Y}"
            / f"month={partition_date:%m}"
        )
        if partition_dir.exists():
            files.extend(partition_dir.glob(pattern))
    return sorted(path for path in files if path.is_file())


def fetch_raw_data(raw_dir: Path, task: ETLTask):
    """根据 (source, dataset) 分发到具体数据源实现。"""
    if task.source == "tushare":
        t_client = TushareClient()
        df = t_client.fetch_tushare_raw(raw_dir, task)
        path = build_raw_path(raw_dir, task)
        print(df)
        print(path)
    else:
        raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")


def _iter_months(start_date: date, end_date: date) -> Iterable[date]:
    """按月迭代日期范围。"""
    if start_date > end_date:
        raise ValueError("开始日期不能晚于结束日期")

    year = start_date.year
    month = start_date.month
    while (year, month) <= (end_date.year, end_date.month):
        yield date(year, month, 1)
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
