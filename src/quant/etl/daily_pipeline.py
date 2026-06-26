"""每日 ETL 管线编排。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

from loguru import logger

from quant.config import QuantConfig
from quant.data.db import DuckDBManager
from quant.data.repository import QuantRepository
from quant.etl.etl_model import ETLTask
from quant.etl.fetch import fetch_raw_data
from quant.etl.inspector import find_missing_dates
from quant.etl.load import load_raw_data

DailyPipelineAction = Literal["fetch", "load", "missing"]
TRADE_CALENDAR_EXCHANGE = "SSE"


@dataclass(frozen=True)
class DailyPipelineStep:
    """每日管线中的一个固定步骤。"""

    action: DailyPipelineAction
    dataset: str
    name: str


@dataclass(frozen=True)
class DailyPipelineStepResult:
    """每日管线步骤执行结果。"""

    name: str
    action: DailyPipelineAction
    dataset: str
    success: bool
    message: str


@dataclass(frozen=True)
class DailyPipelineResult:
    """每日管线执行结果。"""

    trade_date: date
    source: str
    is_open: bool
    steps: tuple[DailyPipelineStepResult, ...]


PRE_DAILY_STEPS = (
    DailyPipelineStep("fetch", "trade-calendar", "fetch trade-calendar"),
    DailyPipelineStep("load", "trade-calendar", "load trade-calendar"),
    DailyPipelineStep("fetch", "stock-basic", "fetch stock-basic"),
    DailyPipelineStep("load", "stock-basic", "load stock-basic"),
)

OPEN_DAY_STEPS = (
    DailyPipelineStep("fetch", "daily-ohlcv", "fetch daily-ohlcv"),
    DailyPipelineStep("fetch", "stock-st", "fetch stock-st"),
    DailyPipelineStep("fetch", "stk-limit", "fetch stk-limit"),
    DailyPipelineStep("fetch", "suspend-d", "fetch suspend-d"),
    DailyPipelineStep("load", "daily-ohlcv", "load daily-ohlcv"),
    DailyPipelineStep("fetch", "adj-factor", "fetch adj-factor"),
    DailyPipelineStep("load", "adj-factor", "load adj-factor"),
    DailyPipelineStep("fetch", "daily-basic", "fetch daily-basic"),
    DailyPipelineStep("load", "daily-basic", "load daily-basic"),
    DailyPipelineStep("missing", "daily-ohlcv", "missing daily-ohlcv"),
    DailyPipelineStep("missing", "adj-factor", "missing adj-factor"),
    DailyPipelineStep("missing", "daily-basic", "missing daily-basic"),
    DailyPipelineStep("missing", "stock-st", "missing stock-st"),
    DailyPipelineStep("missing", "stk-limit", "missing stk-limit"),
    DailyPipelineStep("missing", "suspend-d", "missing suspend-d"),
)


def run_daily_pipeline(
    config: QuantConfig,
    source: str,
    trade_date: date,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> DailyPipelineResult:
    """按固定顺序执行单日 Tushare 数据管线。"""
    if source != "tushare":
        raise NotImplementedError(f"暂未实现每日管线: source={source}")

    all_steps = (*PRE_DAILY_STEPS, *OPEN_DAY_STEPS)
    results: list[DailyPipelineStepResult] = []
    for step_index, step in enumerate(PRE_DAILY_STEPS, start=1):
        results.append(
            _run_daily_step(
                config,
                source,
                trade_date,
                step,
                step_index=step_index,
                total_steps=len(all_steps),
                force=force,
                dry_run=dry_run,
            )
        )

    is_open = _load_trade_date_open_state(config, trade_date)
    if not is_open:
        logger.bind(module="etl", pipeline="daily").info(
            "当日休市,已跳过日频数据: date={}",
            trade_date,
        )
        return DailyPipelineResult(
            trade_date=trade_date,
            source=source,
            is_open=False,
            steps=tuple(results),
        )

    for offset, step in enumerate(OPEN_DAY_STEPS, start=len(PRE_DAILY_STEPS) + 1):
        results.append(
            _run_daily_step(
                config,
                source,
                trade_date,
                step,
                step_index=offset,
                total_steps=len(all_steps),
                force=force,
                dry_run=dry_run,
            )
        )

    return DailyPipelineResult(
        trade_date=trade_date,
        source=source,
        is_open=True,
        steps=tuple(results),
    )


def _run_daily_step(
    config: QuantConfig,
    source: str,
    trade_date: date,
    step: DailyPipelineStep,
    *,
    step_index: int,
    total_steps: int,
    force: bool,
    dry_run: bool,
) -> DailyPipelineStepResult:
    """执行单个每日管线步骤。"""
    step_logger = logger.bind(
        module="etl",
        pipeline="daily",
        action=step.action,
        dataset=step.dataset,
        trade_date=trade_date,
    )
    step_logger.info(
        "每日管线步骤开始: step={}/{}, action={}, dataset={}, trade_date={}",
        step_index,
        total_steps,
        step.action,
        step.dataset,
        trade_date,
    )

    task = _build_daily_task(
        dataset=step.dataset,
        source=source,
        trade_date=trade_date,
        force=force,
        dry_run=dry_run if step.action != "missing" else False,
    )
    try:
        message = _execute_step(config, task, step.action)
    except Exception:
        step_logger.exception(
            "每日管线步骤失败: step={}/{}, action={}, dataset={}, trade_date={}",
            step_index,
            total_steps,
            step.action,
            step.dataset,
            trade_date,
        )
        raise

    step_logger.info(
        "每日管线步骤完成: step={}/{}, action={}, dataset={}, message={}",
        step_index,
        total_steps,
        step.action,
        step.dataset,
        message,
    )
    return DailyPipelineStepResult(
        name=step.name,
        action=step.action,
        dataset=step.dataset,
        success=True,
        message=message,
    )


def _execute_step(
    config: QuantConfig,
    task: ETLTask,
    action: DailyPipelineAction,
) -> str:
    """调用已有 ETL 能力执行步骤。"""
    if action == "fetch":
        output_paths = fetch_raw_data(config, task)
        return f"raw_files={len(output_paths)}"
    if action == "load":
        row_count = load_raw_data(config, task)
        return f"row_count={row_count}"
    if action == "missing":
        result = find_missing_dates(config, task)
        return f"missing_dates={len(result.missing_dates)}"
    raise ValueError(f"不支持的每日管线动作: action={action}")


def _build_daily_task(
    *,
    dataset: str,
    source: str,
    trade_date: date,
    force: bool,
    dry_run: bool,
) -> ETLTask:
    """构造单日 ETLTask。"""
    return ETLTask(
        dataset=dataset,
        source=source,
        start_date=trade_date,
        end_date=trade_date,
        force=force,
        dry_run=dry_run,
    )


def _load_trade_date_open_state(config: QuantConfig, trade_date: date) -> bool:
    """读取当日是否开市,缺少日历记录时直接失败。"""
    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
        repository = QuantRepository(conn)
        rows = repository.get_trade_calendar(
            trade_date,
            trade_date,
            exchange=TRADE_CALENDAR_EXCHANGE,
        )

    if not rows:
        raise ValueError(f"交易日历缺少当日记录: date={trade_date}")
    return bool(rows[0]["is_open"])
