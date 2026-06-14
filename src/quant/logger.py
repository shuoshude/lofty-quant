"""项目日志配置。"""

from __future__ import annotations

import sys
from pathlib import Path
from types import TracebackType

from loguru import logger

from .config import QuantConfig, load_config
from .utils import resolve_log_dir

LOG_FILE_SIZE_BYTES = 10 * 1024 * 1024
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} | {message}"


def setup_logger(
    config: QuantConfig | None = None,
    *,
    level: str = "INFO",
    log_dir: Path | str | None = None,
    enable_console: bool = True,
) -> None:
    """配置项目的 loguru 输出目标。"""
    resolved_config = config or load_config()
    log_dir = resolve_log_dir(log_dir, resolved_config)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger.remove()
    if enable_console:
        # 控制台日志同步写入, 避免测试或短任务退出时异步队列写到已关闭的 stderr。
        logger.add(sys.stderr, level=level, format=LOG_FORMAT)

    logger.add(
        log_dir / "lofty-quant_{time:YYYY-MM-DD}.log",
        level=level,
        format=LOG_FORMAT,
        rotation=LOG_FILE_SIZE_BYTES,
        encoding="utf-8",
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )
    logger.add(
        log_dir / "etl_{time:YYYY-MM-DD}.log",
        level="INFO",
        format=LOG_FORMAT,
        rotation=LOG_FILE_SIZE_BYTES,
        filter=lambda record: record["extra"].get("module") == "etl",
        encoding="utf-8",
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )


def log_exception(
    exc_type: type[BaseException],
    exc: BaseException,
    traceback: TracebackType | None,
) -> None:
    """记录未捕获异常及其堆栈信息。"""
    logger.opt(exception=(exc_type, exc, traceback)).critical("未捕获异常")
