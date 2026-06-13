"""lofty-quant ETL 轻量命令入口。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated

import typer
from loguru import logger

from quant.config import QuantConfig, load_config
from quant.data.db import DuckDBManager
from quant.etl import ETLTask, fetch_raw_data, get_manifest_status, load_raw_data
from quant.logger import setup_logger

app = typer.Typer(help="lofty-quant ETL 轻量入口")

DatasetArg = Annotated[str, typer.Argument(help="数据集名称, 例如 daily-ohlcv 或 trade-calendar")]
SourceOption = Annotated[str, typer.Option("--source", "-s", help="数据源名称")]
ConfigDirOption = Annotated[str | None, typer.Option("--config-dir", help="配置目录")]
EnvironmentOption = Annotated[str | None, typer.Option("--environment", "-e", help="配置环境")]
LogLevelOption = Annotated[str, typer.Option("--log-level", help="日志级别")]
ExchangeOption = Annotated[str | None, typer.Option("--exchange", help="交易所")]


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
        output_path = fetch_raw_data(config, task)
    except NotImplementedError as exc:
        raise typer.BadParameter(str(exc)) from exc
    logger.bind(module="etl").info("原始数据落盘完成: {}", output_path)
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
    except NotImplementedError as exc:
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
        output_path = fetch_raw_data(config, task)
        row_count = load_raw_data(config, task)
    except NotImplementedError as exc:
        raise typer.BadParameter(str(exc)) from exc
    logger.bind(module="etl").info("历史回填完成: raw={}, 行数={}", output_path, row_count)
    typer.echo(f"历史回填完成: raw={output_path}, row_count={row_count}")


@app.command()
def status(
    dataset: DatasetArg,
    source: Annotated[str | None, typer.Option("--source", "-s", help="可选数据源名称")] = None,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """查看简单加载状态。"""
    config = _setup_runtime(config_dir, environment, log_level)
    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
        state = get_manifest_status(conn, dataset=dataset, source=source)

    latest_trade_date = _format_optional_value(state["latest_trade_date"])
    latest_loaded_at = _format_optional_value(state["latest_loaded_at"])
    typer.echo(f"数据集: {dataset}")
    typer.echo(f"数据源: {source or '*'}")
    typer.echo(f"加载记录数: {state['loaded_count']}")
    typer.echo(f"最新交易日: {latest_trade_date}")
    typer.echo(f"最近加载时间: {latest_loaded_at}")


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
