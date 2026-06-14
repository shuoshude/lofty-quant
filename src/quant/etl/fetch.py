"""ETL 原始数据拉取和 raw CSV 落盘工具。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from loguru import logger
from pandas import DataFrame

from quant.config import QuantConfig
from quant.etl.etl_model import ETLTask
from quant.utils import build_raw_path, is_daily_file_raw_dataset, is_single_file_raw_dataset

RawFetchResult = DataFrame | Mapping[date, DataFrame]


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

    existing_files = sorted(path for path in files if path.is_file())
    if is_daily_file_raw_dataset(task):
        return [
            path
            for path in existing_files
            if _is_daily_raw_file_in_range(path, task)
        ]
    return existing_files


def fetch_raw_data(config: QuantConfig, task: ETLTask) -> tuple[Path, ...]:
    """根据 (source, dataset) 拉取 DataFrame 并写入 raw CSV。"""
    logger.bind(module="etl").info(
        "开始拉取原始数据: dataset={}, source={}, start_date={}, end_date={}, exchange={}",
        task.dataset,
        task.source,
        task.start_date,
        task.end_date,
        task.exchange or "-",
    )
    if task.source == "tushare":
        raw_result = _fetch_tushare_raw(config, task)
    else:
        raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")

    return _write_raw_result(config, task, raw_result)


def _write_raw_result(
    config: QuantConfig,
    task: ETLTask,
    raw_result: RawFetchResult,
) -> tuple[Path, ...]:
    """将 source 返回的 raw DataFrame 写入一个或多个 CSV。"""
    if isinstance(raw_result, Mapping):
        paths: list[Path] = []
        for trade_date, df in sorted(raw_result.items()):
            daily_task = task.model_copy(
                update={"start_date": trade_date, "end_date": trade_date},
            )
            paths.append(_write_single_raw_frame(config, daily_task, df))
        return tuple(paths)

    return (_write_single_raw_frame(config, task, raw_result),)


def _write_single_raw_frame(config: QuantConfig, task: ETLTask, df: DataFrame) -> Path:
    """写入单个 raw CSV。"""
    path = build_raw_path(config.paths.raw_dir, task)
    if task.dry_run:
        logger.bind(module="etl").info(
            "试运行: 跳过 raw CSV 写入, 行数={}, 路径={}",
            len(df.index),
            path,
        )
        return path

    row_count = write_raw_csv(path, df)
    logger.bind(module="etl").info("raw CSV 写入完成: 路径={}, 行数={}", path, row_count)
    return path


def _fetch_tushare_raw(config: QuantConfig, task: ETLTask) -> RawFetchResult:
    """懒加载 Tushare 数据源, 避免 source 与 raw 工具循环导入。"""
    from quant.etl.sources.tushare_source import TushareClient

    return TushareClient(config).fetch_tushare_raw(task)


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


def _is_daily_raw_file_in_range(path: Path, task: ETLTask) -> bool:
    """判断日线 raw 文件名中的交易日是否落在任务范围内。"""
    file_date = _parse_daily_raw_file_date(path, task)
    return file_date is not None and task.start_date <= file_date <= task.end_date


def _parse_daily_raw_file_date(path: Path, task: ETLTask) -> date | None:
    """从 daily-ohlcv_tushare_YYYYMMDD.csv 这类文件名解析交易日。"""
    prefix = f"{task.dataset}_{task.source}_"
    if not path.stem.startswith(prefix):
        return None

    date_text = path.stem.removeprefix(prefix)
    if len(date_text) != 8 or not date_text.isdigit():
        return None
    try:
        return datetime.strptime(date_text, "%Y%m%d").date()
    except ValueError:
        return None
