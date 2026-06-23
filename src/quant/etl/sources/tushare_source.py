"""Tushare 数据源适配器。"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import pandas as pd
import tushare as ts
from duckdb import CatalogException
from loguru import logger
from pandas import DataFrame
from pydantic import ValidationError

from quant.config import QuantConfig
from quant.data.db import DuckDBManager
from quant.data.fields import (
    ADJ_FACTOR_COLUMNS,
    DAILY_BASIC_COLUMNS,
    DAILY_OHLCV_COLUMNS,
    TUSHARE_ADJ_FACTOR_RAW_COLUMNS,
    TUSHARE_ADJ_FACTOR_REQUIRED_COLUMNS,
    TUSHARE_DAILY_BASIC_RAW_COLUMNS,
    TUSHARE_DAILY_BASIC_REQUIRED_COLUMNS,
    TUSHARE_DAILY_OHLCV_RAW_COLUMNS,
    TUSHARE_DAILY_OHLCV_REQUIRED_COLUMNS,
)
from quant.data.repository import QuantRepository
from quant.data.schemas import AdjFactorRecord, DailyBasicRecord, DailyOHLCVRecord
from quant.etl.etl_model import ETLTask
from quant.etl.fetch import find_raw_files, read_raw_csv
from quant.etl.processed import archive_daily_year, load_daily_raw_csv_to_monthly_parquet
from quant.etl.storage import replace_table_dataframe
from quant.utils import build_raw_path

TUSHARE_REQUEST_SLEEP_SECONDS = 0.2
MISSING_TRADE_CALENDAR_MESSAGE = "请先加载交易日历后再拉取日线行情"


@dataclass(frozen=True)
class TushareDailyFetchSpec:
    """Tushare 日频 raw 拉取编排参数。"""

    dataset: str
    label: str
    api_method_name: str
    raw_columns: Sequence[str]
    use_fields: bool = False


@dataclass(frozen=True)
class TushareDailyLoadSpec:
    """Tushare 日频 processed 加载编排参数。"""

    dataset: str
    label: str
    processed_dataset: str
    processed_columns: Sequence[str]
    normalize_frame: Callable[[DataFrame, ETLTask], DataFrame]
    missing_raw_message: str


TUSHARE_DAILY_FETCH_SPECS: dict[str, TushareDailyFetchSpec] = {
    "daily-ohlcv": TushareDailyFetchSpec(
        dataset="daily-ohlcv",
        label="日线行情",
        api_method_name="daily",
        raw_columns=TUSHARE_DAILY_OHLCV_RAW_COLUMNS,
    ),
    "adj-factor": TushareDailyFetchSpec(
        dataset="adj-factor",
        label="复权因子",
        api_method_name="adj_factor",
        raw_columns=TUSHARE_ADJ_FACTOR_RAW_COLUMNS,
    ),
    "daily-basic": TushareDailyFetchSpec(
        dataset="daily-basic",
        label="每日指标",
        api_method_name="daily_basic",
        raw_columns=TUSHARE_DAILY_BASIC_RAW_COLUMNS,
        use_fields=True,
    ),
}


class _TushareApiClient:
    """Tushare API 客户端, 只负责外部接口调用。"""

    def __init__(self, config: QuantConfig) -> None:
        tushare_token = config.secrets.tushare_token
        if not tushare_token:
            raise ValueError("请在环境变量中设置 LOFTY_QUANT__SECRETS__TUSHARE_TOKEN")
        ts.set_token(tushare_token)
        self._pro_api: Any = ts.pro_api()

    def fetch_trade_calendar(self, task: ETLTask) -> DataFrame:
        """调用 Tushare 交易日历接口。"""
        exchange = task.exchange or ""
        logger.bind(module="etl").info(
            "开始调用 Tushare 交易日历接口: exchange={}, start_date={}, end_date={}",
            exchange or "all",
            task.start_date,
            task.end_date,
        )
        _sleep_before_request()
        result = self._pro_api.trade_cal(
            exchange=exchange,
            start_date=task.start_date.strftime("%Y%m%d"),
            end_date=task.end_date.strftime("%Y%m%d"),
        )
        df = cast(DataFrame, result)
        logger.bind(module="etl").info("Tushare 交易日历接口返回完成: 行数={}", len(df.index))
        return df

    def fetch_daily_frame(self, trade_date: date, spec: TushareDailyFetchSpec) -> DataFrame:
        """调用 Tushare 日频接口并返回单个交易日 DataFrame。"""
        trade_date_text = trade_date.strftime("%Y%m%d")
        fields = ",".join(spec.raw_columns) if spec.use_fields else None
        _sleep_before_request()
        logger.bind(module="etl").info(
            "开始调用 Tushare {}接口: trade_date={}",
            spec.label,
            trade_date_text,
        )
        try:
            api_method = getattr(self._pro_api, spec.api_method_name)
            if fields is None:
                result = api_method(trade_date=trade_date_text)
            else:
                result = api_method(trade_date=trade_date_text, fields=fields)
        except Exception:
            logger.bind(module="etl").exception(
                "Tushare {}接口调用失败: trade_date={}",
                spec.label,
                trade_date_text,
            )
            raise

        df = cast(DataFrame, result)
        logger.bind(module="etl").info(
            "Tushare {}接口返回完成: trade_date={}, 行数={}",
            spec.label,
            trade_date_text,
            len(df.index),
        )
        return df


class TushareSource:
    """Tushare 数据源适配器, 统一承载 fetch/load/archive。"""

    def __init__(self, config: QuantConfig) -> None:
        self._config = config
        self._api_client: _TushareApiClient | None = None

    def fetch_raw(self, task: ETLTask) -> DataFrame | Iterator[tuple[date, DataFrame]]:
        """按数据集拉取 Tushare 原始 DataFrame。"""
        if task.dataset == "trade-calendar":
            return self.fetch_trade_calendar(task)
        if task.dataset == "daily-ohlcv":
            return self.fetch_daily_ohlcv(task)
        if task.dataset == "adj-factor":
            return self.fetch_adj_factor(task)
        if task.dataset == "daily-basic":
            return self.fetch_daily_basic(task)
        raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")

    def load_raw(self, task: ETLTask) -> int:
        """按数据集加载 Tushare raw CSV。"""
        if task.dataset == "trade-calendar":
            return load_trade_calendar(self._config, task)
        if task.dataset == "daily-ohlcv":
            return load_daily_ohlcv(self._config, task)
        if task.dataset == "adj-factor":
            return load_adj_factor(self._config, task)
        if task.dataset == "daily-basic":
            return load_daily_basic(self._config, task)
        raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")

    def archive_daily_ohlcv_year(self, year: int) -> Path:
        """将某个已结束年份的月度日线 Parquet 归档为年文件。"""
        year_path = archive_daily_year(
            self._config.paths.processed_dir,
            "ohlcv",
            year,
            key_columns=["ts_code", "trade_date"],
            columns=DAILY_OHLCV_COLUMNS,
        )
        logger.bind(module="etl").info(
            "日线行情年度归档完成: year={}, 路径={}",
            year,
            year_path,
        )
        return year_path

    def fetch_trade_calendar(self, task: ETLTask) -> DataFrame:
        """拉取交易日历原始数据。"""
        return self._api().fetch_trade_calendar(task)

    def fetch_daily_ohlcv(self, task: ETLTask) -> Iterator[tuple[date, DataFrame]]:
        """按本地交易日历逐日拉取 Tushare 日线行情原始数据。"""
        return self._fetch_daily_dataset(task, TUSHARE_DAILY_FETCH_SPECS["daily-ohlcv"])

    def fetch_adj_factor(self, task: ETLTask) -> Iterator[tuple[date, DataFrame]]:
        """按本地交易日历逐日拉取 Tushare 复权因子原始数据。"""
        return self._fetch_daily_dataset(task, TUSHARE_DAILY_FETCH_SPECS["adj-factor"])

    def fetch_daily_basic(self, task: ETLTask) -> Iterator[tuple[date, DataFrame]]:
        """按本地交易日历逐日拉取 Tushare 每日指标原始数据。"""
        return self._fetch_daily_dataset(task, TUSHARE_DAILY_FETCH_SPECS["daily-basic"])

    def _fetch_daily_dataset(
        self,
        task: ETLTask,
        spec: TushareDailyFetchSpec,
    ) -> Iterator[tuple[date, DataFrame]]:
        """按本地交易日历逐日拉取 Tushare 日频 raw 数据。"""
        trade_dates = _load_open_trade_dates(self._config, task)
        logger.bind(module="etl").info(
            "开始拉取 Tushare {}: exchange={}, 开市日数量={}",
            spec.label,
            task.exchange or "SSE",
            len(trade_dates),
        )

        for trade_date in trade_dates:
            trade_date_text = trade_date.strftime("%Y%m%d")
            if _should_skip_existing_daily_raw(self._config, task, trade_date):
                yield trade_date, pd.DataFrame(columns=list(spec.raw_columns))
                continue

            df = self._api().fetch_daily_frame(trade_date, spec)
            if df.empty:
                logger.bind(module="etl").info(
                    "Tushare {}返回为空, 写出空 raw CSV 表头: trade_date={}",
                    spec.label,
                    trade_date_text,
                )
                yield trade_date, pd.DataFrame(columns=list(spec.raw_columns))
                continue

            yield trade_date, df

        logger.bind(module="etl").info(
            "Tushare {}按日拉取完成: 交易日数量={}",
            spec.label,
            len(trade_dates),
        )

    def _api(self) -> _TushareApiClient:
        """懒加载 Tushare API 客户端, 避免离线 load/archive 依赖 token。"""
        if self._api_client is None:
            self._api_client = _TushareApiClient(self._config)
        return self._api_client


def load_trade_calendar(config: QuantConfig, task: ETLTask) -> int:
    """读取 Tushare 交易日历 raw CSV, 标准化后写入 DuckDB。"""
    raw_path = build_raw_path(config.paths.raw_dir, task)
    if not raw_path.is_file():
        raise FileNotFoundError(f"未找到 raw CSV 文件: {raw_path}")

    logger.bind(module="etl").info("开始加载 Tushare 交易日历 raw: 路径={}", raw_path)
    raw_df = read_raw_csv(raw_path)
    calendar_df = normalize_trade_calendar_df(raw_df, task)
    logger.bind(module="etl").info(
        "交易日历标准化完成: 行数={}, 起始日期={}, 结束日期={}",
        len(calendar_df.index),
        calendar_df["cal_date"].min() if not calendar_df.empty else "-",
        calendar_df["cal_date"].max() if not calendar_df.empty else "-",
    )

    # 交易日历以目标表为事实源, 同一范围重跑时直接覆盖旧数据。
    if task.exchange:
        delete_where = "exchange = ? AND cal_date BETWEEN ? AND ?"
        delete_params: Sequence[Any] = [task.exchange.upper(), task.start_date, task.end_date]
    else:
        delete_where = "cal_date BETWEEN ? AND ?"
        delete_params = [task.start_date, task.end_date]

    row_count = replace_table_dataframe(
        config.paths.database_path,
        config.paths.processed_dir,
        table="dim_trade_calendar",
        df=calendar_df,
        columns=["exchange", "cal_date", "is_open", "pretrade_date"],
        delete_where=delete_where,
        delete_params=delete_params,
    )
    logger.bind(module="etl").info(
        "交易日历写入 DuckDB 完成: 表=dim_trade_calendar, 行数={}",
        row_count,
    )
    return row_count


def load_daily_ohlcv(config: QuantConfig, task: ETLTask) -> int:
    """读取 Tushare 日线 raw CSV, 标准化后写入月度 processed Parquet。"""
    return _load_daily_dataset(config, task, _daily_load_specs()["daily-ohlcv"])


def load_adj_factor(config: QuantConfig, task: ETLTask) -> int:
    """读取 Tushare 复权因子 raw CSV, 标准化后写入月度 processed Parquet。"""
    return _load_daily_dataset(config, task, _daily_load_specs()["adj-factor"])


def load_daily_basic(config: QuantConfig, task: ETLTask) -> int:
    """读取 Tushare 每日指标 raw CSV, 标准化后写入月度 processed Parquet。"""
    return _load_daily_dataset(config, task, _daily_load_specs()["daily-basic"])


def _daily_load_specs() -> dict[str, TushareDailyLoadSpec]:
    """返回 Tushare 日频 processed 加载配置。"""
    return {
        "daily-ohlcv": TushareDailyLoadSpec(
            dataset="daily-ohlcv",
            label="日线行情",
            processed_dataset="ohlcv",
            processed_columns=DAILY_OHLCV_COLUMNS,
            normalize_frame=normalize_daily_ohlcv_df,
            missing_raw_message="未找到日线行情 raw CSV 文件",
        ),
        "adj-factor": TushareDailyLoadSpec(
            dataset="adj-factor",
            label="复权因子",
            processed_dataset="adj_factor",
            processed_columns=ADJ_FACTOR_COLUMNS,
            normalize_frame=normalize_adj_factor_df,
            missing_raw_message="未找到复权因子 raw CSV 文件",
        ),
        "daily-basic": TushareDailyLoadSpec(
            dataset="daily-basic",
            label="每日指标",
            processed_dataset="daily_basic",
            processed_columns=DAILY_BASIC_COLUMNS,
            normalize_frame=normalize_daily_basic_df,
            missing_raw_message="未找到每日指标 raw CSV 文件",
        ),
    }


def _load_daily_dataset(
    config: QuantConfig,
    task: ETLTask,
    spec: TushareDailyLoadSpec,
) -> int:
    """读取日频 raw CSV, 标准化后写入月度 processed Parquet。"""
    raw_files = find_raw_files(config.paths.raw_dir, task)
    if not raw_files:
        raise FileNotFoundError(spec.missing_raw_message)

    result = load_daily_raw_csv_to_monthly_parquet(
        raw_files,
        config.paths.processed_dir,
        spec.processed_dataset,
        read_frame=read_raw_csv,
        normalize_frame=lambda raw_df: spec.normalize_frame(raw_df, task),
        date_column="trade_date",
        key_columns=["ts_code", "trade_date"],
        columns=spec.processed_columns,
        dry_run=task.dry_run,
    )
    for output_path, written_count in sorted(result.written_paths.items()):
        logger.bind(module="etl").info(
            "{} processed 月文件写入完成: 路径={}, 新增行数={}",
            spec.label,
            output_path,
            written_count,
        )

    return result.row_count


def normalize_trade_calendar_df(raw_df: DataFrame, task: ETLTask) -> DataFrame:
    """将 Tushare 交易日历 raw DataFrame 向量化转换为项目标准表结构。"""
    _require_columns(raw_df, ["cal_date", "is_open"])
    default_exchange = (task.exchange or "SSE").upper()

    output = pd.DataFrame(index=raw_df.index)
    output["exchange"] = _normalize_exchange_series(raw_df, default_exchange)
    output["cal_date"] = _parse_date_series(raw_df["cal_date"], field_name="cal_date")
    output["is_open"] = _parse_is_open_series(raw_df["is_open"])

    if "pretrade_date" in raw_df.columns:
        output["pretrade_date"] = _parse_date_series(
            raw_df["pretrade_date"],
            field_name="pretrade_date",
            required=False,
        )
    else:
        output["pretrade_date"] = None

    return output[["exchange", "cal_date", "is_open", "pretrade_date"]]


def normalize_daily_ohlcv_df(raw_df: DataFrame, task: ETLTask) -> DataFrame:
    """将 Tushare 日线 raw DataFrame 向量化转换为项目标准表结构。"""
    _require_daily_ohlcv_columns(raw_df)
    if raw_df.empty:
        return pd.DataFrame(columns=DAILY_OHLCV_COLUMNS)

    output = pd.DataFrame(index=raw_df.index)
    output["ts_code"] = raw_df["ts_code"].astype("string").str.strip()
    output["trade_date"] = _parse_date_series(raw_df["trade_date"], field_name="trade_date")
    _validate_daily_ohlcv_date_range(output["trade_date"], task)

    for field_name in ("open", "high", "low", "close", "amount"):
        output[field_name] = _parse_numeric_series(raw_df[field_name], field_name=field_name)
    output["volume"] = _parse_numeric_series(raw_df["vol"], field_name="vol")

    for field_name in ("pre_close", "change", "pct_chg"):
        if field_name in raw_df.columns:
            output[field_name] = _parse_numeric_series(
                raw_df[field_name],
                field_name=field_name,
                required=False,
            )
        else:
            output[field_name] = None

    output["is_suspended"] = False
    output["is_st"] = False
    output["limit_status"] = "none"
    return _validate_daily_ohlcv_contract(output.loc[:, list(DAILY_OHLCV_COLUMNS)])


def normalize_adj_factor_df(raw_df: DataFrame, task: ETLTask) -> DataFrame:
    """将 Tushare 复权因子 raw DataFrame 转换为项目标准表结构。"""
    _require_adj_factor_columns(raw_df)
    if raw_df.empty:
        return pd.DataFrame(columns=ADJ_FACTOR_COLUMNS)

    output = pd.DataFrame(index=raw_df.index)
    output["ts_code"] = raw_df["ts_code"].astype("string").str.strip()
    output["trade_date"] = _parse_date_series(raw_df["trade_date"], field_name="trade_date")
    _validate_adj_factor_date_range(output["trade_date"], task)
    output["cumulative_factor"] = _parse_numeric_series(
        raw_df["adj_factor"],
        field_name="adj_factor",
    )

    return _validate_adj_factor_contract(output.loc[:, list(ADJ_FACTOR_COLUMNS)])


def normalize_daily_basic_df(raw_df: DataFrame, task: ETLTask) -> DataFrame:
    """将 Tushare 每日指标 raw DataFrame 转换为项目标准表结构。"""
    _require_daily_basic_columns(raw_df)
    if raw_df.empty:
        return pd.DataFrame(columns=DAILY_BASIC_COLUMNS)

    output = pd.DataFrame(index=raw_df.index)
    output["ts_code"] = raw_df["ts_code"].astype("string").str.strip()
    output["trade_date"] = _parse_date_series(raw_df["trade_date"], field_name="trade_date")
    _validate_daily_basic_date_range(output["trade_date"], task)

    for field_name in DAILY_BASIC_COLUMNS:
        if field_name in {"ts_code", "trade_date"}:
            continue
        if field_name in raw_df.columns:
            output[field_name] = _parse_numeric_series(
                raw_df[field_name],
                field_name=field_name,
                required=False,
            )
        else:
            output[field_name] = None

    # Tushare daily_basic 的特殊标记在 processed 层转成项目标准语义。
    for field_name in ("pe", "pe_ttm"):
        output[field_name] = output[field_name].fillna(-1.0)
    for field_name in ("volume_ratio", "dv_ratio", "dv_ttm"):
        output[field_name] = output[field_name].fillna(0.0).mask(output[field_name] < 0, 0.0)
    _normalize_daily_basic_anomaly_fields(output)

    return _validate_daily_basic_contract(output.loc[:, list(DAILY_BASIC_COLUMNS)])


def _validate_daily_ohlcv_contract(df: DataFrame) -> DataFrame:
    """使用项目日线数据契约进行最终校验。"""
    errors: list[str] = []
    for row in df.to_dict(orient="records"):
        row_data = {str(key): value for key, value in row.items()}
        try:
            DailyOHLCVRecord(**row_data)
        except ValidationError as exc:
            errors.append(_format_daily_ohlcv_validation_error(row_data, exc))
            if len(errors) >= 3:
                break

    if errors:
        raise ValueError(f"日线行情数据契约校验失败: {'; '.join(errors)}")
    return df


def _format_daily_ohlcv_validation_error(row: dict[str, Any], exc: ValidationError) -> str:
    """格式化 Pydantic 校验错误, 便于定位异常行。"""
    error_messages = []
    for error in exc.errors()[:3]:
        location = ".".join(str(part) for part in error.get("loc", ())) or "-"
        error_messages.append(f"{location}: {error.get('msg', '-')}")

    return (
        f"ts_code={row.get('ts_code', '-')}, "
        f"trade_date={row.get('trade_date', '-')}, "
        f"错误={', '.join(error_messages)}"
    )


def _validate_adj_factor_contract(df: DataFrame) -> DataFrame:
    """使用项目复权因子契约进行最终校验。"""
    errors: list[str] = []
    for row in df.to_dict(orient="records"):
        row_data = {str(key): value for key, value in row.items()}
        try:
            AdjFactorRecord(**row_data)
        except ValidationError as exc:
            errors.append(_format_adj_factor_validation_error(row_data, exc))
            if len(errors) >= 3:
                break

    if errors:
        raise ValueError(f"复权因子数据契约校验失败: {'; '.join(errors)}")
    return df


def _format_adj_factor_validation_error(row: dict[str, Any], exc: ValidationError) -> str:
    """格式化复权因子校验错误, 便于定位异常行。"""
    error_messages = []
    for error in exc.errors()[:3]:
        location = ".".join(str(part) for part in error.get("loc", ())) or "-"
        error_messages.append(f"{location}: {error.get('msg', '-')}")

    return (
        f"ts_code={row.get('ts_code', '-')}, "
        f"trade_date={row.get('trade_date', '-')}, "
        f"错误={', '.join(error_messages)}"
    )


def _validate_daily_basic_contract(df: DataFrame) -> DataFrame:
    """使用项目每日指标契约进行最终校验。"""
    errors: list[str] = []
    for row in df.to_dict(orient="records"):
        row_data = {str(key): value for key, value in row.items()}
        try:
            DailyBasicRecord(**row_data)
        except ValidationError as exc:
            errors.append(_format_daily_basic_validation_error(row_data, exc))
            if len(errors) >= 3:
                break

    if errors:
        raise ValueError(f"每日指标数据契约校验失败: {'; '.join(errors)}")
    return df


def _format_daily_basic_validation_error(row: dict[str, Any], exc: ValidationError) -> str:
    """格式化每日指标校验错误, 便于定位异常行。"""
    error_messages = []
    for error in exc.errors()[:3]:
        location = ".".join(str(part) for part in error.get("loc", ())) or "-"
        error_messages.append(f"{location}: {error.get('msg', '-')}")

    return (
        f"ts_code={row.get('ts_code', '-')}, "
        f"trade_date={row.get('trade_date', '-')}, "
        f"错误={', '.join(error_messages)}"
    )


def _normalize_daily_basic_anomaly_fields(df: DataFrame) -> None:
    """归一化每日指标中的异常指标字段, 并记录错误日志。"""
    for field_name in (
        "turnover_rate",
        "turnover_rate_f",
        "total_share",
        "free_share",
        "float_share",
        "total_mv",
        "circ_mv",
    ):
        anomaly_mask = df[field_name].isna() | df[field_name].le(0)
        if not anomaly_mask.any():
            continue

        sample_rows = (
            df.loc[anomaly_mask, ["ts_code", "trade_date", field_name]]
            .head(3)
            .to_dict(orient="records")
        )
        logger.bind(module="etl").error(
            "每日指标 raw 存在异常指标字段, 已在 processed 入库时置为 0: "
            "字段={}, 异常行数={}, 样例={}",
            field_name,
            int(anomaly_mask.sum()),
            sample_rows,
        )
        df.loc[anomaly_mask, field_name] = 0.0


def _require_columns(df: DataFrame, columns: Sequence[str]) -> None:
    """校验 raw DataFrame 必须包含指定字段。"""
    missing_columns = [column for column in columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"交易日历 raw 缺少字段: {missing_columns}")


def _require_daily_ohlcv_columns(df: DataFrame) -> None:
    """校验日线行情 raw DataFrame 必须包含核心字段。"""
    missing_columns = [
        column for column in TUSHARE_DAILY_OHLCV_REQUIRED_COLUMNS if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"日线行情 raw 缺少字段: {missing_columns}")


def _require_adj_factor_columns(df: DataFrame) -> None:
    """校验复权因子 raw DataFrame 必须包含核心字段。"""
    missing_columns = [
        column for column in TUSHARE_ADJ_FACTOR_REQUIRED_COLUMNS if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"复权因子 raw 缺少字段: {missing_columns}")


def _require_daily_basic_columns(df: DataFrame) -> None:
    """校验每日指标 raw DataFrame 必须包含核心字段。"""
    missing_columns = [
        column for column in TUSHARE_DAILY_BASIC_REQUIRED_COLUMNS if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"每日指标 raw 缺少字段: {missing_columns}")


def _validate_daily_ohlcv_date_range(series: pd.Series, task: ETLTask) -> None:
    """校验 raw 中的交易日是否落在任务范围内。"""
    invalid_mask = (series < task.start_date) | (series > task.end_date)
    if invalid_mask.any():
        invalid_values = series[invalid_mask].head(3).tolist()
        raise ValueError(f"日线行情 raw 日期超出任务范围: {invalid_values}")


def _validate_adj_factor_date_range(series: pd.Series, task: ETLTask) -> None:
    """校验复权因子 raw 中的交易日是否落在任务范围内。"""
    invalid_mask = (series < task.start_date) | (series > task.end_date)
    if invalid_mask.any():
        invalid_values = series[invalid_mask].head(3).tolist()
        raise ValueError(f"复权因子 raw 日期超出任务范围: {invalid_values}")


def _validate_daily_basic_date_range(series: pd.Series, task: ETLTask) -> None:
    """校验每日指标 raw 中的交易日是否落在任务范围内。"""
    invalid_mask = (series < task.start_date) | (series > task.end_date)
    if invalid_mask.any():
        invalid_values = series[invalid_mask].head(3).tolist()
        raise ValueError(f"每日指标 raw 日期超出任务范围: {invalid_values}")


def _normalize_exchange_series(raw_df: DataFrame, default_exchange: str) -> pd.Series:
    """向量化标准化交易所字段。"""
    if "exchange" not in raw_df.columns:
        return pd.Series(default_exchange, index=raw_df.index)

    normalized = raw_df["exchange"].astype("string").str.strip()
    normalized = normalized.mask(_missing_string_mask(normalized), default_exchange)
    return normalized.str.upper()


def _parse_date_series(
    series: pd.Series,
    *,
    field_name: str,
    required: bool = True,
) -> pd.Series:
    """向量化解析 YYYYMMDD 日期字段。"""
    normalized = series.astype("string").str.strip()
    missing_mask = _missing_string_mask(normalized)
    parsed = pd.to_datetime(normalized.mask(missing_mask), format="%Y%m%d", errors="coerce")

    invalid_mask = parsed.isna() if required else parsed.isna() & ~missing_mask
    if invalid_mask.any():
        invalid_values = normalized[invalid_mask].head(3).tolist()
        raise ValueError(f"日期字段 {field_name} 格式无效: {invalid_values}")

    result = parsed.dt.date.astype("object")
    result[parsed.isna()] = None
    return result


def _parse_is_open_series(series: pd.Series) -> pd.Series:
    """向量化解析 Tushare is_open 字段。"""
    normalized = series.astype("string").str.strip()
    missing_mask = _missing_string_mask(normalized)
    if missing_mask.any():
        raise ValueError("is_open 不能为空")

    numeric = pd.to_numeric(normalized, errors="coerce")
    invalid_mask = numeric.isna() | ~numeric.isin([0, 1])
    if invalid_mask.any():
        invalid_values = normalized[invalid_mask].head(3).tolist()
        raise ValueError(f"is_open 只能为 0 或 1: {invalid_values}")
    return numeric.astype("int64").eq(1)


def _parse_numeric_series(
    series: pd.Series,
    *,
    field_name: str,
    required: bool = True,
) -> pd.Series:
    """向量化解析数值字段。"""
    normalized = series.astype("string").str.strip()
    missing_mask = _missing_string_mask(normalized)
    numeric = pd.to_numeric(normalized.mask(missing_mask), errors="coerce")

    invalid_mask = numeric.isna() if required else numeric.isna() & ~missing_mask
    if invalid_mask.any():
        invalid_values = normalized[invalid_mask].head(3).tolist()
        raise ValueError(f"数值字段 {field_name} 格式无效: {invalid_values}")
    return numeric.astype("float64")


def _missing_string_mask(series: pd.Series) -> pd.Series:
    """判断字符串 Series 中的空值。"""
    return series.isna() | series.str.lower().isin({"", "nan", "nat", "none", "<na>"})


def _load_open_trade_dates(config: QuantConfig, task: ETLTask) -> list[date]:
    """从本地交易日历读取任务范围内的开市日。"""
    database_path = config.paths.database_path
    if not database_path.is_file():
        raise ValueError(MISSING_TRADE_CALENDAR_MESSAGE)

    exchange = (task.exchange or "SSE").upper()
    manager = DuckDBManager(database_path, config.paths.processed_dir)
    try:
        with manager.session() as conn:
            repository = QuantRepository(conn)
            trade_dates = repository.get_open_trade_dates(
                task.start_date,
                task.end_date,
                exchange=exchange,
            )
    except CatalogException as exc:
        raise ValueError(MISSING_TRADE_CALENDAR_MESSAGE) from exc

    if not trade_dates:
        raise ValueError(MISSING_TRADE_CALENDAR_MESSAGE)
    return trade_dates


def _should_skip_existing_daily_raw(config: QuantConfig, task: ETLTask, trade_date: date) -> bool:
    """存在日频 raw 且未强制覆盖时,跳过外部接口请求。"""
    if task.force or task.dry_run:
        return False

    daily_task = task.model_copy(update={"start_date": trade_date, "end_date": trade_date})
    raw_path = build_raw_path(config.paths.raw_dir, daily_task)
    if not raw_path.is_file():
        return False

    logger.bind(module="etl").info(
        "日频 raw CSV 已存在, 跳过外部接口请求: dataset={}, 路径={}",
        task.dataset,
        raw_path,
    )
    return True


def _sleep_before_request() -> None:
    """Tushare 请求之间固定等待, 避免超过每分钟 500 次。"""
    time.sleep(TUSHARE_REQUEST_SLEEP_SECONDS)
