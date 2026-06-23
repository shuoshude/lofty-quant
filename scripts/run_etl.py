"""lofty-quant ETL 轻量命令入口。"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from datetime import date, datetime
from typing import Annotated

import typer
from duckdb import DuckDBPyConnection
from loguru import logger

from quant.config import QuantConfig, load_config
from quant.data.db import DuckDBManager
from quant.etl import ETLTask, fetch_raw_data, load_raw_data
from quant.logger import setup_logger

app = typer.Typer(help="lofty-quant ETL 轻量入口")

DatasetArg = Annotated[
    str,
    typer.Argument(help="数据集名称, 例如 daily-ohlcv、daily-basic、adj-factor 或 trade-calendar"),
]
SourceOption = Annotated[str, typer.Option("--source", "-s", help="数据源名称")]
ConfigDirOption = Annotated[str | None, typer.Option("--config-dir", help="配置目录")]
EnvironmentOption = Annotated[str | None, typer.Option("--environment", "-e", help="配置环境")]
LogLevelOption = Annotated[str, typer.Option("--log-level", help="日志级别")]
ExchangeOption = Annotated[str | None, typer.Option("--exchange", help="交易所")]
YearOption = Annotated[int, typer.Option("--year", help="归档年份")]


@app.command()
def fetch(
    dataset: DatasetArg,
    source: SourceOption,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期")] = None,
    exchange: ExchangeOption = None,
    force: Annotated[bool, typer.Option("--force", help="强制重新拉取并覆盖 raw")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="只拉取和校验, 不写 raw")] = False,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """只拉取原始数据并写入 raw。"""
    config = _setup_runtime(config_dir, environment, log_level)
    task = _build_task(dataset, source, start_date, end_date, exchange, force, dry_run)
    try:
        output_paths = fetch_raw_data(config, task)
    except (FileNotFoundError, NotImplementedError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    logger.bind(module="etl").info("原始数据落盘完成: 文件数量={}", len(output_paths))
    for output_path in output_paths:
        typer.echo(f"原始数据落盘完成: {output_path}")


@app.command("load")
def load_command(
    dataset: DatasetArg,
    source: SourceOption,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期")] = None,
    exchange: ExchangeOption = None,
    force: Annotated[bool, typer.Option("--force", help="强制重新加载并覆盖目标范围")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="只转换和校验, 不写目标存储")] = False,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """从 raw 读取、清洗转换后写入目标存储。"""
    config = _setup_runtime(config_dir, environment, log_level)
    task = _build_task(dataset, source, start_date, end_date, exchange, force, dry_run)
    try:
        row_count = load_raw_data(config, task)
    except (FileNotFoundError, NotImplementedError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    logger.bind(module="etl").info("目标存储加载完成: 行数={}", row_count)
    typer.echo(f"目标存储加载完成: row_count={row_count}")


@app.command()
def backfill(
    dataset: DatasetArg,
    source: SourceOption,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期")] = None,
    exchange: ExchangeOption = None,
    force: Annotated[bool, typer.Option("--force", help="强制重跑并覆盖旧数据")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="只拉取和校验, 不写入存储")] = False,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """执行历史回填, 按 fetch -> load 编排。"""
    config = _setup_runtime(config_dir, environment, log_level)
    task = _build_task(dataset, source, start_date, end_date, exchange, force, dry_run)
    try:
        output_paths = fetch_raw_data(config, task)
        row_count = load_raw_data(config, task)
    except (FileNotFoundError, NotImplementedError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    logger.bind(module="etl").info(
        "历史回填完成: raw文件数量={}, 行数={}",
        len(output_paths),
        row_count,
    )
    typer.echo(f"历史回填完成: raw_count={len(output_paths)}, row_count={row_count}")


@app.command()
def archive(
    dataset: DatasetArg,
    source: SourceOption,
    year: YearOption,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """归档 processed 数据。"""
    config = _setup_runtime(config_dir, environment, log_level)
    try:
        if source == "tushare":
            from quant.etl.sources.tushare_source import TushareSource

            output_path = TushareSource(config).archive_year(dataset, year)
        else:
            raise NotImplementedError(f"暂未实现归档: dataset={dataset}, source={source}")
    except (FileNotFoundError, NotImplementedError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    logger.bind(module="etl").info("归档完成: dataset={}, 路径={}", dataset, output_path)
    typer.echo(f"归档完成: {output_path}")


@app.command()
def status(
    dataset: DatasetArg,
    source: Annotated[str | None, typer.Option("--source", "-s", help="可选数据源名称")] = None,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """查看目标数据真实状态。"""
    config = _setup_runtime(config_dir, environment, log_level)
    if dataset == "trade-calendar":
        with _status_session(config) as conn:
            state = _get_trade_calendar_status(conn)
        logger.bind(module="etl").info(
            "目标数据状态查询完成: dataset={}, source={}, row_count={}",
            dataset,
            source or "*",
            state["row_count"],
        )
        typer.echo(f"数据集: {dataset}")
        typer.echo(f"数据源: {source or '*'}")
        typer.echo(f"交易所: {state['exchange']}")
        typer.echo(f"起始日期: {_format_optional_value(state['start_date'])}")
        typer.echo(f"结束日期: {_format_optional_value(state['end_date'])}")
        typer.echo(f"日历行数: {state['row_count']}")
        typer.echo(f"开市天数: {state['open_count']}")
        return

    if dataset == "daily-ohlcv":
        state = _get_daily_ohlcv_status(config)
        logger.bind(module="etl").info(
            "目标数据状态查询完成: dataset={}, source={}, row_count={}",
            dataset,
            source or "*",
            state["row_count"],
        )
        typer.echo(f"数据集: {dataset}")
        typer.echo(f"数据源: {source or '*'}")
        typer.echo(f"起始日期: {_format_optional_value(state['start_date'])}")
        typer.echo(f"结束日期: {_format_optional_value(state['end_date'])}")
        typer.echo(f"行情行数: {state['row_count']}")
        typer.echo(f"交易日数: {state['trade_date_count']}")
        typer.echo(f"证券数: {state['security_count']}")
        return

    if dataset == "adj-factor":
        state = _get_adj_factor_status(config)
        logger.bind(module="etl").info(
            "目标数据状态查询完成: dataset={}, source={}, row_count={}",
            dataset,
            source or "*",
            state["row_count"],
        )
        typer.echo(f"数据集: {dataset}")
        typer.echo(f"数据源: {source or '*'}")
        typer.echo(f"起始日期: {_format_optional_value(state['start_date'])}")
        typer.echo(f"结束日期: {_format_optional_value(state['end_date'])}")
        typer.echo(f"因子行数: {state['row_count']}")
        typer.echo(f"交易日数: {state['trade_date_count']}")
        typer.echo(f"证券数: {state['security_count']}")
        return

    if dataset == "daily-basic":
        state = _get_daily_basic_status(config)
        logger.bind(module="etl").info(
            "目标数据状态查询完成: dataset={}, source={}, row_count={}",
            dataset,
            source or "*",
            state["row_count"],
        )
        typer.echo(f"数据集: {dataset}")
        typer.echo(f"数据源: {source or '*'}")
        typer.echo(f"起始日期: {_format_optional_value(state['start_date'])}")
        typer.echo(f"结束日期: {_format_optional_value(state['end_date'])}")
        typer.echo(f"指标行数: {state['row_count']}")
        typer.echo(f"交易日数: {state['trade_date_count']}")
        typer.echo(f"证券数: {state['security_count']}")
        return

    raise typer.BadParameter(f"暂未实现数据集状态查询: dataset={dataset}")


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


def _get_daily_ohlcv_status(config: QuantConfig) -> dict[str, object]:
    """从日线行情 processed Parquet 聚合真实状态。"""
    return _get_daily_processed_status(
        config,
        dataset_dir_name="ohlcv",
        view_name="v_daily_ohlcv",
    )


def _get_adj_factor_status(config: QuantConfig) -> dict[str, object]:
    """从复权因子 processed Parquet 聚合真实状态。"""
    return _get_daily_processed_status(
        config,
        dataset_dir_name="adj_factor",
        view_name="v_adj_factor",
    )


def _get_daily_basic_status(config: QuantConfig) -> dict[str, object]:
    """从每日指标 processed Parquet 聚合真实状态。"""
    return _get_daily_processed_status(
        config,
        dataset_dir_name="daily_basic",
        view_name="v_daily_basic",
    )


def _get_daily_processed_status(
    config: QuantConfig,
    *,
    dataset_dir_name: str,
    view_name: str,
) -> dict[str, object]:
    """从日频 processed Parquet 聚合真实状态。"""
    dataset_dir = config.paths.processed_dir / dataset_dir_name
    if not list(dataset_dir.glob("**/*.parquet")):
        return {
            "start_date": None,
            "end_date": None,
            "row_count": 0,
            "trade_date_count": 0,
            "security_count": 0,
        }

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
        return {
            "start_date": None,
            "end_date": None,
            "row_count": 0,
            "trade_date_count": 0,
            "security_count": 0,
        }

    start_date, end_date, row_count, trade_date_count, security_count = row
    return {
        "start_date": start_date,
        "end_date": end_date,
        "row_count": int(row_count or 0),
        "trade_date_count": int(trade_date_count or 0),
        "security_count": int(security_count or 0),
    }


def _setup_runtime(
    config_dir: str | None,
    environment: str | None,
    log_level: str,
) -> QuantConfig:
    """加载配置并初始化日志。"""
    config = load_config(config_dir=config_dir, environment=environment)
    setup_logger(config=config, level=log_level)
    return config


def _build_task(
    dataset: str,
    source: str,
    start_date: str | None,
    end_date: str | None,
    exchange: str | None,
    force: bool,
    dry_run: bool,
) -> ETLTask:
    """构造 ETL 任务并校验日期。"""
    parsed_start_date = _require_date(start_date, "--start-date")
    parsed_end_date = _require_date(end_date, "--end-date")
    if parsed_start_date > parsed_end_date:
        raise typer.BadParameter("开始日期不能晚于结束日期")
    return ETLTask(
        dataset=dataset,
        source=source,
        start_date=parsed_start_date,
        end_date=parsed_end_date,
        exchange=exchange,
        force=force,
        dry_run=dry_run,
    )


def _require_date(value: str | None, option_name: str) -> date:
    """解析必填日期参数。"""
    if value is None:
        raise typer.BadParameter(f"{option_name} 不能为空")
    normalized = value.strip()
    for pattern in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, pattern).date()
        except ValueError:
            continue
    raise typer.BadParameter(f"日期格式无效: {value}, 应使用 YYYYMMDD 或 YYYY-MM-DD")


def _format_optional_value(value: object) -> str:
    """格式化可选状态值。"""
    if value is None:
        return "-"
    if isinstance(value, date | datetime):
        return value.isoformat()
    return str(value)

if __name__ == "__main__":
    app()
