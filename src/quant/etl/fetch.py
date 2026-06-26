"""ETL raw 拉取入口。"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from pathlib import Path

from loguru import logger
from pandas import DataFrame

from quant.config import QuantConfig
from quant.etl.etl_model import ETLTask
from quant.etl.raw import write_raw_csv
from quant.utils import build_raw_path

DailyRawFrames = Iterable[tuple[date, DataFrame]]
RawFetchResult = DataFrame | DailyRawFrames


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
    if isinstance(raw_result, DataFrame):
        return (_write_single_raw_frame(config, task, raw_result),)

    paths: list[Path] = []
    for trade_date, df in raw_result:
        daily_task = task.model_copy(
            update={"start_date": trade_date, "end_date": trade_date},
        )
        paths.append(_write_single_raw_frame(config, daily_task, df))
    return tuple(paths)


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

    if path.exists() and not task.force and not _should_overwrite_single_file_raw(task):
        logger.bind(module="etl").info("raw CSV 已存在, 跳过写入: 路径={}", path)
        return path

    row_count = write_raw_csv(path, df)
    logger.bind(module="etl").info("raw CSV 写入完成: 路径={}, 行数={}", path, row_count)
    return path


def _fetch_tushare_raw(config: QuantConfig, task: ETLTask) -> RawFetchResult:
    """懒加载 Tushare 数据源, 避免 source 与 raw 工具循环导入。"""
    from quant.etl.sources.tushare_source import TushareSource

    return TushareSource(config).fetch_raw(task)


def _should_overwrite_single_file_raw(task: ETLTask) -> bool:
    """单文件 raw 是最新请求快照,成功拉取后直接覆盖旧文件。"""
    return task.dataset in {"trade-calendar", "stock-basic"}
