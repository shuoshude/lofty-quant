"""Project logging setup."""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Protocol, TextIO, cast

from loguru import logger

from quant.config import QuantConfig, get_project_root, load_config

if TYPE_CHECKING:
    from loguru import Message

LOG_FILE_SIZE_BYTES = 10 * 1024 * 1024
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} | {message}"


def setup_logger(
    config: QuantConfig | None = None,
    *,
    level: str = "INFO",
    log_dir: Path | str | None = None,
    enable_console: bool = True,
) -> None:
    """Configure loguru sinks for the project."""
    resolved_config = config or load_config()
    resolved_log_dir = _resolve_log_dir(log_dir, resolved_config)
    resolved_log_dir.mkdir(parents=True, exist_ok=True)

    logger.remove()
    if enable_console:
        logger.add(sys.stderr, level=level, format=LOG_FORMAT, enqueue=True)

    logger.add(
        resolved_log_dir / "lofty-quant_{time:YYYY-MM-DD}.log",
        level=level,
        format=LOG_FORMAT,
        rotation=_daily_or_size_rotation(LOG_FILE_SIZE_BYTES),
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
    """Log an uncaught exception with traceback details."""
    logger.opt(exception=(exc_type, exc, traceback)).critical("Uncaught exception")


def _resolve_log_dir(log_dir: Path | str | None, config: QuantConfig) -> Path:
    """Resolve the runtime log directory."""
    if log_dir is None:
        return config.paths.log_dir

    path = Path(log_dir).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (get_project_root() / path).resolve()


class SupportsTell(Protocol):
    """File object protocol used by loguru rotation callbacks."""

    def tell(self) -> int:
        """Return the current file position."""


class SupportsDate(Protocol):
    """Datetime-like object stored in loguru records."""

    def date(self) -> date:
        """Return the calendar date."""


def _daily_or_size_rotation(max_bytes: int) -> Callable[[Message, TextIO], bool]:
    """Create a loguru rotation predicate for daily files and size chunks."""
    current_date: date | None = None

    def should_rotate(message: Message, file: TextIO) -> bool:
        nonlocal current_date

        record = _message_record(message)
        record_date = record["time"].date()
        if current_date is None:
            current_date = record_date

        if record_date != current_date:
            current_date = record_date
            return True

        return _file_position(file) + len(str(message).encode("utf-8")) > max_bytes

    return should_rotate


def _message_record(message: Message) -> Mapping[str, SupportsDate]:
    """Return the runtime loguru message record."""
    return cast(Mapping[str, SupportsDate], object.__getattribute__(message, "record"))


def _file_position(file: SupportsTell) -> int:
    """Return the current file position from loguru's sink file object."""
    position = file.tell()
    if not isinstance(position, int):
        raise TypeError("log file position must be an integer")
    return position
