"""ETL 数据状态和缺失日期检查工具。"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import cast

from duckdb import DuckDBPyConnection

from quant.config import QuantConfig
from quant.data.db import DuckDBManager
from quant.data.repository import QuantRepository
from quant.etl.etl_model import ETLTask
from quant.etl.raw import find_raw_files
from quant.utils import parse_daily_raw_file_date

TRADE_CALENDAR_EXCHANGE = "SSE"
MISSING_TRADE_CALENDAR_MESSAGE = "请先加载交易日历后再检查缺失日期"

PROCESSED_DAILY_DATASETS = {
    "daily-ohlcv": ("ohlcv", "v_daily_ohlcv"),
    "adj-factor": ("adj_factor", "v_adj_factor"),
    "daily-basic": ("daily_basic", "v_daily_basic"),
}
RAW_ONLY_DAILY_DATASETS = {"stock-st", "stk-limit", "suspend-d"}


@dataclass(frozen=True)
class MissingDateResult:
    """数据集日期级缺失检查结果。"""

    dataset: str
    source: str
    start_date: date
    end_date: date
    expected_dates: tuple[date, ...]
    existing_dates: tuple[date, ...]
    missing_dates: tuple[date, ...]


def get_dataset_status(
    config: QuantConfig,
    dataset: str,
    source: str | None = None,
) -> dict[str, object]:
    """返回目标数据真实状态。"""
    _ = source
    if dataset == "trade-calendar":
        with _status_session(config) as conn:
            return _get_trade_calendar_status(conn)
    if dataset == "daily-ohlcv":
        return _get_daily_processed_status(
            config,
            dataset_dir_name="ohlcv",
            view_name="v_daily_ohlcv",
        )
    if dataset == "adj-factor":
        return _get_daily_processed_status(
            config,
            dataset_dir_name="adj_factor",
            view_name="v_adj_factor",
        )
    if dataset == "daily-basic":
        return _get_daily_processed_status(
            config,
            dataset_dir_name="daily_basic",
            view_name="v_daily_basic",
        )
    if dataset == "stock-basic":
        with _status_session(config) as conn:
            return _get_stock_basic_status(conn)
    raise NotImplementedError(f"暂未实现数据集状态查询: dataset={dataset}")


def find_missing_dates(config: QuantConfig, task: ETLTask) -> MissingDateResult:
    """查询数据集在指定范围内缺失的交易日。"""
    if task.dataset == "stock-basic":
        raise NotImplementedError("stock-basic 是快照型数据集, 不支持交易日缺失日期检查")

    if task.dataset == "trade-calendar":
        expected_dates = tuple(_iter_calendar_dates(task.start_date, task.end_date))
        existing_dates = _load_trade_calendar_existing_dates(config, task)
        return _build_missing_result(task, expected_dates, existing_dates)

    if task.dataset in PROCESSED_DAILY_DATASETS:
        _dataset_dir_name, view_name = PROCESSED_DAILY_DATASETS[task.dataset]
        _ensure_trade_calendar_coverage(config, task)
        expected_dates = _load_open_trade_dates(config, task)
        existing_dates = _load_processed_existing_dates(config, view_name, task)
        return _build_missing_result(task, expected_dates, existing_dates)

    if task.dataset in RAW_ONLY_DAILY_DATASETS:
        _ensure_trade_calendar_coverage(config, task)
        expected_dates = _load_open_trade_dates(config, task)
        existing_dates = _load_raw_existing_dates(config.paths.raw_dir, task)
        return _build_missing_result(task, expected_dates, existing_dates)

    raise NotImplementedError(
        f"暂未实现缺失日期检查: dataset={task.dataset}, source={task.source}"
    )


@contextmanager
def _status_session(config: QuantConfig) -> Generator[DuckDBPyConnection, None, None]:
    """提供状态查询连接, 统一走 DuckDBManager 初始化 schema 和视图。"""
    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
        yield conn


def _get_trade_calendar_status(conn: DuckDBPyConnection) -> dict[str, object]:
    """从交易日历目标表聚合真实状态。"""
    row = conn.execute(
        """
        SELECT
            COALESCE(exchange, '*') AS exchange,
            MIN(cal_date) AS start_date,
            MAX(cal_date) AS end_date,
            COUNT(*) AS row_count,
            SUM(CASE WHEN is_open THEN 1 ELSE 0 END) AS open_count
        FROM dim_trade_calendar
        GROUP BY exchange
        ORDER BY exchange
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return {
            "exchange": "-",
            "start_date": None,
            "end_date": None,
            "row_count": 0,
            "open_count": 0,
        }

    exchange, start_date, end_date, row_count, open_count = row
    return {
        "exchange": exchange,
        "start_date": start_date,
        "end_date": end_date,
        "row_count": int(row_count),
        "open_count": int(open_count or 0),
    }


def _get_daily_processed_status(
    config: QuantConfig,
    *,
    dataset_dir_name: str,
    view_name: str,
) -> dict[str, object]:
    """从日频 processed Parquet 聚合真实状态。"""
    dataset_dir = config.paths.processed_dir / dataset_dir_name
    if not list(dataset_dir.glob("**/*.parquet")):
        return _empty_daily_status()

    with _status_session(config) as conn:
        row = conn.execute(
            f"""
            SELECT
                MIN(trade_date) AS start_date,
                MAX(trade_date) AS end_date,
                COUNT(*) AS row_count,
                COUNT(DISTINCT trade_date) AS trade_date_count,
                COUNT(DISTINCT ts_code) AS security_count
            FROM {view_name}
            """
        ).fetchone()

    if row is None:
        return _empty_daily_status()

    start_date, end_date, row_count, trade_date_count, security_count = row
    return {
        "start_date": start_date,
        "end_date": end_date,
        "row_count": int(row_count or 0),
        "trade_date_count": int(trade_date_count or 0),
        "security_count": int(security_count or 0),
    }


def _get_stock_basic_status(conn: DuckDBPyConnection) -> dict[str, object]:
    """从证券主数据表聚合真实状态。"""
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT exchange) AS exchange_count,
            SUM(CASE WHEN list_status = 'L' THEN 1 ELSE 0 END) AS listed_count,
            SUM(CASE WHEN list_status = 'D' THEN 1 ELSE 0 END) AS delisted_count,
            SUM(CASE WHEN list_status = 'P' THEN 1 ELSE 0 END) AS paused_count
        FROM dim_security
        """
    ).fetchone()
    if row is None:
        return {
            "row_count": 0,
            "exchange_count": 0,
            "listed_count": 0,
            "delisted_count": 0,
            "paused_count": 0,
        }

    row_count, exchange_count, listed_count, delisted_count, paused_count = row
    return {
        "row_count": int(row_count or 0),
        "exchange_count": int(exchange_count or 0),
        "listed_count": int(listed_count or 0),
        "delisted_count": int(delisted_count or 0),
        "paused_count": int(paused_count or 0),
    }


def _load_trade_calendar_existing_dates(config: QuantConfig, task: ETLTask) -> tuple[date, ...]:
    """读取交易日历目标表中已存在的自然日期。"""
    with _status_session(config) as conn:
        repository = QuantRepository(conn)
        rows = repository.get_trade_calendar(
            task.start_date,
            task.end_date,
            exchange=TRADE_CALENDAR_EXCHANGE,
        )
    return tuple(cast(date, row["cal_date"]) for row in rows)


def _ensure_trade_calendar_coverage(config: QuantConfig, task: ETLTask) -> None:
    """确保交易日历自然日覆盖完整检查范围。"""
    expected_dates = tuple(_iter_calendar_dates(task.start_date, task.end_date))
    existing_dates = set(_load_trade_calendar_existing_dates(config, task))
    missing_dates = tuple(value for value in expected_dates if value not in existing_dates)
    if not missing_dates:
        return

    sample_dates = ", ".join(value.isoformat() for value in missing_dates[:10])
    suffix = ", ..." if len(missing_dates) > 10 else ""
    raise ValueError(
        "交易日历未覆盖检查范围, 请先补齐交易日历: "
        f"缺失日期=[{sample_dates}{suffix}]"
    )


def _load_open_trade_dates(config: QuantConfig, task: ETLTask) -> tuple[date, ...]:
    """读取任务范围内的开市日。"""
    with _status_session(config) as conn:
        repository = QuantRepository(conn)
        trade_dates = repository.get_open_trade_dates(
            task.start_date,
            task.end_date,
            exchange=TRADE_CALENDAR_EXCHANGE,
        )

    if not trade_dates:
        raise ValueError(MISSING_TRADE_CALENDAR_MESSAGE)
    return tuple(trade_dates)


def _load_processed_existing_dates(
    config: QuantConfig,
    view_name: str,
    task: ETLTask,
) -> tuple[date, ...]:
    """读取日频 processed 视图中已存在的交易日。"""
    with _status_session(config) as conn:
        if not _relation_exists(conn, view_name):
            return ()
        rows = conn.execute(
            f"""
            SELECT DISTINCT trade_date
            FROM {view_name}
            WHERE trade_date BETWEEN ? AND ?
            ORDER BY trade_date
            """,
            [task.start_date, task.end_date],
        ).fetchall()
    return tuple(cast(date, row[0]) for row in rows)


def _load_raw_existing_dates(raw_dir: Path, task: ETLTask) -> tuple[date, ...]:
    """从 raw 日文件名中提取已存在交易日。"""
    dates = {
        file_date
        for path in find_raw_files(raw_dir, task)
        if (file_date := parse_daily_raw_file_date(path, task)) is not None
    }
    return tuple(sorted(dates))


def _build_missing_result(
    task: ETLTask,
    expected_dates: tuple[date, ...],
    existing_dates: tuple[date, ...],
) -> MissingDateResult:
    """根据应有日期和已有日期计算缺失结果。"""
    existing_set = set(existing_dates)
    missing_dates = tuple(value for value in expected_dates if value not in existing_set)
    return MissingDateResult(
        dataset=task.dataset,
        source=task.source,
        start_date=task.start_date,
        end_date=task.end_date,
        expected_dates=expected_dates,
        existing_dates=tuple(value for value in existing_dates if value in set(expected_dates)),
        missing_dates=missing_dates,
    )


def _iter_calendar_dates(start_date: date, end_date: date) -> Generator[date, None, None]:
    """逐日生成自然日。"""
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def _empty_daily_status() -> dict[str, object]:
    """返回空日频状态。"""
    return {
        "start_date": None,
        "end_date": None,
        "row_count": 0,
        "trade_date_count": 0,
        "security_count": 0,
    }


def _relation_exists(conn: DuckDBPyConnection, relation_name: str) -> bool:
    """返回 DuckDB 表或视图是否存在。"""
    result = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [relation_name],
    ).fetchone()
    return bool(result and result[0])
