from __future__ import annotations

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
