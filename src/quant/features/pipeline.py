"""日频因子计算 Pipeline。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from quant.config import QuantConfig
from quant.data.db import DuckDBManager
from quant.data.repository import QuantRepository
from quant.features.base import FactorMetadata
from quant.features.processing import (
    PROCESSED_FACTOR_COLUMNS,
    FactorProcessor,
    process_factor_result,
)
from quant.features.registry import build_default_registry
from quant.features.storage import write_factor_results
from quant.features.technical import TECHNICAL_CALCULATORS

FactorCalculator = Callable[[pl.DataFrame], pl.DataFrame]
FACTOR_KEY_COLUMNS = ("ts_code", "trade_date", "factor_name", "factor_version")
A_SHARE_CALENDAR_START = date(1990, 1, 1)


@dataclass(frozen=True, slots=True)
class FactorRunSummary:
    """描述一次因子 Pipeline 的输入、输出和写入结果。"""

    factor_names: tuple[str, ...]
    factor_version: str
    start_date: date
    end_date: date
    warmup_start_date: date
    input_row_count: int
    output_row_count: int
    written_paths: tuple[Path, ...]
    missing_value_rate: float
    valid_rate: float
    processor: FactorProcessor
    dry_run: bool


def run_factor_pipeline(
    config: QuantConfig,
    factor_names: Sequence[str],
    start_date: date,
    end_date: date,
    *,
    factor_version: str = "v1",
    processor: FactorProcessor = "raw",
    dry_run: bool = False,
) -> FactorRunSummary:
    """计算、处理、校验并按需写入一批日频因子。"""
    normalized_names = _validate_task(factor_names, start_date, end_date, processor)
    definitions = _resolve_factor_definitions(normalized_names, factor_version)
    required_fields = tuple(
        dict.fromkeys(
            field for metadata, _calculator in definitions for field in metadata.required_fields
        )
    )
    max_lookback = max(metadata.lookback_days for metadata, _calculator in definitions)

    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    try:
        manager.initialize()
        with manager.session() as conn:
            repository = QuantRepository(conn)
            warmup_start_date = _get_warmup_start_date(
                repository,
                start_date,
                max_lookback,
            )
            panel = repository.get_daily_panel(
                warmup_start_date,
                end_date,
                required_fields,
                adjustment="hfq",
            )

        if panel.is_empty():
            raise ValueError(
                f"因子输入面板为空: start_date={warmup_start_date}, end_date={end_date}"
            )

        processed_results = [
            process_factor_result(calculator(panel), metadata, processor)
            for metadata, calculator in definitions
        ]
        output = (
            pl.concat(processed_results, how="vertical")
            .filter(pl.col("trade_date").is_between(start_date, end_date, closed="both"))
            .sort(["ts_code", "trade_date", "factor_name", "factor_version"])
        )
        _validate_output(output, start_date, end_date)

        written_paths: tuple[Path, ...] = ()
        if not dry_run:
            written = write_factor_results(config.paths.processed_dir, output.to_pandas())
            written_paths = tuple(sorted(written))
            manager.refresh_views()

        output_row_count = output.height
        missing_value_rate = output["factor_value"].null_count() / output_row_count
        valid_count = output.filter(pl.col("quality_status") == "valid").height
        valid_rate = valid_count / output_row_count
        return FactorRunSummary(
            factor_names=normalized_names,
            factor_version=factor_version,
            start_date=start_date,
            end_date=end_date,
            warmup_start_date=warmup_start_date,
            input_row_count=panel.height,
            output_row_count=output_row_count,
            written_paths=written_paths,
            missing_value_rate=missing_value_rate,
            valid_rate=valid_rate,
            processor=processor,
            dry_run=dry_run,
        )
    finally:
        manager.close()


def _validate_task(
    factor_names: Sequence[str],
    start_date: date,
    end_date: date,
    processor: FactorProcessor,
) -> tuple[str, ...]:
    """校验不依赖外部数据的任务参数。"""
    if isinstance(factor_names, str):
        raise ValueError("factor_names 必须是因子名称序列,不能传入字符串")
    normalized_names = tuple(factor_names)
    if not normalized_names:
        raise ValueError("factor_names 不能为空")
    if len(set(normalized_names)) != len(normalized_names):
        raise ValueError("factor_names 不能重复")
    if start_date > end_date:
        raise ValueError("start_date 不能晚于 end_date")
    if processor not in ("raw", "rank_pct"):
        raise ValueError(f"不支持的因子 Processor: {processor}")
    return normalized_names


def _resolve_factor_definitions(
    factor_names: tuple[str, ...],
    factor_version: str,
) -> tuple[tuple[FactorMetadata, FactorCalculator], ...]:
    """解析元数据及其不可变 Calculator 注册。"""
    registry = build_default_registry()
    definitions: list[tuple[FactorMetadata, FactorCalculator]] = []
    for factor_name in factor_names:
        metadata = registry.get(factor_name, factor_version)
        calculator = TECHNICAL_CALCULATORS.get((factor_name, factor_version))
        if calculator is None:
            raise NotImplementedError(
                f"因子 Calculator 尚未实现: name={factor_name}, version={factor_version}"
            )
        definitions.append((metadata, calculator))
    return tuple(definitions)


def _get_warmup_start_date(
    repository: QuantRepository,
    start_date: date,
    lookback_days: int,
) -> date:
    """从交易日历选择请求开始日前第 N 个开市日。"""
    if lookback_days == 0:
        return start_date
    if start_date <= A_SHARE_CALENDAR_START:
        raise ValueError(f"交易日历不足以计算 warmup: required={lookback_days}, available=0")

    previous_trade_dates = repository.get_open_trade_dates(
        A_SHARE_CALENDAR_START,
        start_date - timedelta(days=1),
    )
    if len(previous_trade_dates) < lookback_days:
        raise ValueError(
            "交易日历不足以计算 warmup: "
            f"required={lookback_days}, available={len(previous_trade_dates)}"
        )
    return previous_trade_dates[-lookback_days]


def _validate_output(output: pl.DataFrame, start_date: date, end_date: date) -> None:
    """检查写入前的基础列、日期和唯一键契约。"""
    missing_columns = [
        column for column in PROCESSED_FACTOR_COLUMNS if column not in output.columns
    ]
    if missing_columns:
        raise ValueError(f"Pipeline 输出缺少字段: {missing_columns}")
    if output.is_empty():
        raise ValueError(f"请求区间没有因子结果: start_date={start_date}, end_date={end_date}")
    if output.select(FACTOR_KEY_COLUMNS).is_duplicated().any():
        raise ValueError(f"Pipeline 输出包含重复键: key_columns={FACTOR_KEY_COLUMNS}")
    outside_range = output.filter(
        ~pl.col("trade_date").is_between(start_date, end_date, closed="both")
    )
    if not outside_range.is_empty():
        raise ValueError("Pipeline 输出包含请求区间外的交易日")
