"""lofty-quant ETL 轻量命令入口。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated

import typer
from loguru import logger

from quant.config import QuantConfig, load_config
from quant.etl import ETLTask, fetch_raw_data, load_raw_data
from quant.etl.daily_pipeline import run_daily_pipeline
from quant.etl.inspector import find_missing_dates, get_dataset_status
from quant.logger import setup_logger

app = typer.Typer(help="lofty-quant ETL 轻量入口")

DatasetArg = Annotated[
    str,
    typer.Argument(
        help=(
            "数据集名称, 例如 stock-basic、daily-ohlcv、daily-basic、"
            "adj-factor、stock-st、stk-limit、suspend-d 或 trade-calendar"
        )
    ),
]
SourceOption = Annotated[str, typer.Option("--source", "-s", help="数据源名称")]
ConfigDirOption = Annotated[str | None, typer.Option("--config-dir", help="配置目录")]
EnvironmentOption = Annotated[str | None, typer.Option("--environment", "-e", help="配置环境")]
LogLevelOption = Annotated[str, typer.Option("--log-level", help="日志级别")]
YearOption = Annotated[int, typer.Option("--year", help="归档年份")]


@app.command()
def fetch(
    dataset: DatasetArg,
    source: SourceOption,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期")] = None,
    force: Annotated[bool, typer.Option("--force", help="强制重新拉取并覆盖 raw")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="只拉取和校验, 不写 raw")] = False,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """只拉取原始数据并写入 raw。"""
    config = _setup_runtime(config_dir, environment, log_level)
    task = _build_task(dataset, source, start_date, end_date, force, dry_run)
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
    force: Annotated[bool, typer.Option("--force", help="强制重新加载并覆盖目标范围")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="只转换和校验, 不写目标存储")] = False,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """从 raw 读取、清洗转换后写入目标存储。"""
    config = _setup_runtime(config_dir, environment, log_level)
    task = _build_task(dataset, source, start_date, end_date, force, dry_run)
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
    force: Annotated[bool, typer.Option("--force", help="强制重跑并覆盖旧数据")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="只拉取和校验, 不写入存储")] = False,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """执行历史回填, 按 fetch -> load 编排。"""
    config = _setup_runtime(config_dir, environment, log_level)
    task = _build_task(dataset, source, start_date, end_date, force, dry_run)
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
    try:
        state = get_dataset_status(config, dataset, source=source)
    except (FileNotFoundError, NotImplementedError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    logger.bind(module="etl").info(
        "目标数据状态查询完成: dataset={}, source={}, row_count={}",
        dataset,
        source or "*",
        state["row_count"],
    )
    if dataset == "trade-calendar":
        typer.echo(f"数据集: {dataset}")
        typer.echo(f"数据源: {source or '*'}")
        typer.echo(f"交易所: {state['exchange']}")
        typer.echo(f"起始日期: {_format_optional_value(state['start_date'])}")
        typer.echo(f"结束日期: {_format_optional_value(state['end_date'])}")
        typer.echo(f"日历行数: {state['row_count']}")
        typer.echo(f"开市天数: {state['open_count']}")
        return

    if dataset == "daily-ohlcv":
        typer.echo(f"数据集: {dataset}")
        typer.echo(f"数据源: {source or '*'}")
        typer.echo(f"起始日期: {_format_optional_value(state['start_date'])}")
        typer.echo(f"结束日期: {_format_optional_value(state['end_date'])}")
        typer.echo(f"行情行数: {state['row_count']}")
        typer.echo(f"交易日数: {state['trade_date_count']}")
        typer.echo(f"证券数: {state['security_count']}")
        return

    if dataset == "adj-factor":
        typer.echo(f"数据集: {dataset}")
        typer.echo(f"数据源: {source or '*'}")
        typer.echo(f"起始日期: {_format_optional_value(state['start_date'])}")
        typer.echo(f"结束日期: {_format_optional_value(state['end_date'])}")
        typer.echo(f"因子行数: {state['row_count']}")
        typer.echo(f"交易日数: {state['trade_date_count']}")
        typer.echo(f"证券数: {state['security_count']}")
        return

    if dataset == "daily-basic":
        typer.echo(f"数据集: {dataset}")
        typer.echo(f"数据源: {source or '*'}")
        typer.echo(f"起始日期: {_format_optional_value(state['start_date'])}")
        typer.echo(f"结束日期: {_format_optional_value(state['end_date'])}")
        typer.echo(f"指标行数: {state['row_count']}")
        typer.echo(f"交易日数: {state['trade_date_count']}")
        typer.echo(f"证券数: {state['security_count']}")
        return

    if dataset == "stock-basic":
        typer.echo(f"数据集: {dataset}")
        typer.echo(f"数据源: {source or '*'}")
        typer.echo(f"证券总数: {state['row_count']}")
        typer.echo(f"交易所数量: {state['exchange_count']}")
        typer.echo(f"上市数量: {state['listed_count']}")
        typer.echo(f"退市数量: {state['delisted_count']}")
        typer.echo(f"暂停上市数量: {state['paused_count']}")
        return

    raise typer.BadParameter(f"暂未实现数据集状态查询: dataset={dataset}")


@app.command()
def missing(
    dataset: DatasetArg,
    source: SourceOption,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期")] = None,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """查看指定日期范围内的数据集缺失日期。"""
    config = _setup_runtime(config_dir, environment, log_level)
    task = _build_task(dataset, source, start_date, end_date, force=False, dry_run=False)
    try:
        result = find_missing_dates(config, task)
    except (FileNotFoundError, NotImplementedError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    logger.bind(module="etl").info(
        "缺失日期检查完成: dataset={}, source={}, expected={}, existing={}, missing={}",
        result.dataset,
        result.source,
        len(result.expected_dates),
        len(result.existing_dates),
        len(result.missing_dates),
    )
    typer.echo(f"数据集: {result.dataset}")
    typer.echo(f"数据源: {result.source}")
    typer.echo(
        "检查范围: "
        f"{result.start_date.isoformat()} 至 {result.end_date.isoformat()}"
    )
    typer.echo(f"应有日期数: {len(result.expected_dates)}")
    typer.echo(f"已有日期数: {len(result.existing_dates)}")
    typer.echo(f"缺失日期数: {len(result.missing_dates)}")
    if result.missing_dates:
        typer.echo("缺失日期:")
        for missing_date in result.missing_dates:
            typer.echo(missing_date.isoformat())
    else:
        typer.echo("缺失日期: 无")


@app.command()
def daily(
    source: SourceOption,
    run_date: Annotated[str | None, typer.Option("--date", help="管线日期")] = None,
    force: Annotated[bool, typer.Option("--force", help="强制重新拉取并覆盖数据")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="只拉取和校验, 不写入存储")] = False,
    config_dir: ConfigDirOption = None,
    environment: EnvironmentOption = None,
    log_level: LogLevelOption = "INFO",
) -> None:
    """执行单日盘后数据管线。"""
    config = _setup_runtime(config_dir, environment, log_level)
    trade_date = _parse_optional_date(run_date, "--date") or date.today()
    try:
        result = run_daily_pipeline(
            config,
            source,
            trade_date,
            force=force,
            dry_run=dry_run,
        )
    except (FileNotFoundError, NotImplementedError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    success_count = sum(1 for step in result.steps if step.success)
    failure_count = len(result.steps) - success_count
    if not result.is_open:
        typer.echo(f"当日休市,已跳过日频数据: date={result.trade_date.isoformat()}")
    typer.echo(
        "每日管线完成: "
        f"date={result.trade_date.isoformat()}, "
        f"source={result.source}, "
        f"is_open={result.is_open}"
    )
    typer.echo(f"步骤: {len(result.steps)}, 成功: {success_count}, 失败: {failure_count}")


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
    force: bool,
    dry_run: bool,
) -> ETLTask:
    """构造 ETL 任务并校验日期。"""
    if dataset == "stock-basic":
        default_date = date.today()
        parsed_start_date = _parse_optional_date(start_date, "--start-date") or default_date
        parsed_end_date = _parse_optional_date(end_date, "--end-date") or parsed_start_date
    else:
        parsed_start_date = _require_date(start_date, "--start-date")
        parsed_end_date = _require_date(end_date, "--end-date")
    if parsed_start_date > parsed_end_date:
        raise typer.BadParameter("开始日期不能晚于结束日期")
    return ETLTask(
        dataset=dataset,
        source=source,
        start_date=parsed_start_date,
        end_date=parsed_end_date,
        force=force,
        dry_run=dry_run,
    )


def _parse_optional_date(value: str | None, option_name: str) -> date | None:
    """解析可选日期参数。"""
    if value is None:
        return None
    return _parse_date(value, option_name)


def _require_date(value: str | None, option_name: str) -> date:
    """解析必填日期参数。"""
    if value is None:
        raise typer.BadParameter(f"{option_name} 不能为空")
    return _parse_date(value, option_name)


def _parse_date(value: str, _option_name: str) -> date:
    """解析日期文本。"""
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
