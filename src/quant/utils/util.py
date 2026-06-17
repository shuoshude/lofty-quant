from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from quant.config import QuantConfig
    from quant.etl import ETLTask

SINGLE_FILE_RAW_DATASETS = {"trade-calendar"}
DAILY_FILE_RAW_DATASETS = {"daily-ohlcv"}


def get_project_root(start: Path | None = None) -> Path:
    """向上查找包含 pyproject.toml 的项目根目录。"""
    current = (start or Path.cwd()).expanduser().resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("无法找到包含 pyproject.toml 的项目根目录")


def resolve_path(path: Path | str, base_dir: Path) -> Path:
    """按给定基准目录解析路径。"""
    expanded = Path(path).expanduser()
    if expanded.is_absolute():
        return expanded.resolve()
    return (base_dir / expanded).resolve()


def build_raw_path(raw_dir: Path, task: ETLTask, *, suffix: str = "csv") -> Path:
    """生成 ETL raw 文件路径。"""
    normalized_suffix = suffix.lstrip(".")
    base_dir = raw_dir.expanduser().resolve() / task.source / task.dataset

    if is_single_file_raw_dataset(task):
        filename = f"{task.dataset}_{task.source}.{normalized_suffix}"
        return base_dir / filename

    if is_daily_file_raw_dataset(task):
        partition_dir = base_dir / f"year={task.start_date:%Y}" / f"month={task.start_date:%m}"
        filename = f"{task.dataset}_{task.source}_{task.start_date:%Y%m%d}.{normalized_suffix}"
        return partition_dir / filename

    partition_dir = base_dir / f"year={task.start_date:%Y}" / f"month={task.start_date:%m}"
    filename = (
        f"{task.dataset}_{task.source}_{task.start_date:%Y%m%d}_"
        f"{task.end_date:%Y%m%d}.{normalized_suffix}"
    )
    return partition_dir / filename


def iter_raw_partition_dirs(raw_dir: Path, task: ETLTask) -> Iterable[Path]:
    """按任务日期范围迭代 raw 月分区目录。"""
    base_dir = raw_dir.expanduser().resolve() / task.source / task.dataset
    for partition_date in _iter_months(task.start_date, task.end_date):
        yield base_dir / f"year={partition_date:%Y}" / f"month={partition_date:%m}"


def parse_daily_raw_file_date(path: Path, task: ETLTask) -> date | None:
    """从 daily raw 文件名中解析交易日。"""
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


def is_single_file_raw_dataset(task: ETLTask) -> bool:
    """判断数据集 raw 是否使用单文件布局。"""
    return task.dataset in SINGLE_FILE_RAW_DATASETS


def is_daily_file_raw_dataset(task: ETLTask) -> bool:
    """判断数据集 raw 是否按交易日单文件布局。"""
    return task.dataset in DAILY_FILE_RAW_DATASETS


def resolve_log_dir(log_dir: Path | str | None, config: QuantConfig) -> Path:
    """解析运行日志目录。"""
    if log_dir is None:
        return config.paths.log_dir

    return resolve_path(log_dir, get_project_root())


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
