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

from quant.config import QuantConfig
from quant.data.db import DuckDBManager
from quant.data.fields import (
    ADJ_FACTOR_COLUMNS,
    DAILY_BASIC_COLUMNS,
    DAILY_OHLCV_COLUMNS,
    SECURITY_COLUMNS,
    TUSHARE_ADJ_FACTOR_RAW_COLUMNS,
    TUSHARE_DAILY_BASIC_RAW_COLUMNS,
    TUSHARE_DAILY_OHLCV_RAW_COLUMNS,
    TUSHARE_STK_LIMIT_RAW_COLUMNS,
    TUSHARE_STOCK_BASIC_RAW_COLUMNS,
    TUSHARE_STOCK_ST_RAW_COLUMNS,
    TUSHARE_SUSPEND_D_RAW_COLUMNS,
)
from quant.data.repository import QuantRepository
from quant.etl.etl_model import ETLTask
from quant.etl.fetch import find_raw_files, read_raw_csv
from quant.etl.processed import archive_daily_year, load_daily_raw_csv_to_monthly_parquet
from quant.etl.sources.tushare_normalizers import (
    normalize_adj_factor_df,
    normalize_daily_basic_df,
    normalize_daily_ohlcv_df,
    normalize_trade_calendar_df,
)
from quant.etl.storage import replace_table_dataframe
from quant.utils import build_raw_path, parse_daily_raw_file_date

TUSHARE_REQUEST_SLEEP_SECONDS = 0.2
TRADE_CALENDAR_EXCHANGE = "SSE"
MISSING_TRADE_CALENDAR_MESSAGE = "请先加载交易日历后再拉取日线行情"
STOCK_BASIC_LIST_STATUSES = ("L", "D", "P")


@dataclass(frozen=True)
class TushareDailyFetchSpec:
    """Tushare 日频 raw 拉取编排参数。"""

    dataset: str
    label: str
    raw_columns: Sequence[str]


@dataclass(frozen=True)
class TushareDailyLoadSpec:
    """Tushare 日频 processed 加载编排参数。"""

    dataset: str
    label: str
    processed_dataset: str
    processed_columns: Sequence[str]
    normalize_frame: Callable[[DataFrame, ETLTask, Path], DataFrame]
    missing_raw_message: str


def _normalize_daily_ohlcv_for_load(
    raw_df: DataFrame,
    task: ETLTask,
    _raw_path: Path,
) -> DataFrame:
    """标准化日线行情 raw。"""
    return normalize_daily_ohlcv_df(raw_df, task)


def _normalize_adj_factor_for_load(
    raw_df: DataFrame,
    task: ETLTask,
    _raw_path: Path,
) -> DataFrame:
    """标准化复权因子 raw。"""
    return normalize_adj_factor_df(raw_df, task)


def _normalize_daily_basic_for_load(
    raw_df: DataFrame,
    task: ETLTask,
    _raw_path: Path,
) -> DataFrame:
    """标准化每日指标 raw。"""
    return normalize_daily_basic_df(raw_df, task)


TUSHARE_DAILY_FETCH_SPECS: dict[str, TushareDailyFetchSpec] = {
    "daily-ohlcv": TushareDailyFetchSpec(
        dataset="daily-ohlcv",
        label="日线行情",
        raw_columns=TUSHARE_DAILY_OHLCV_RAW_COLUMNS,
    ),
    "adj-factor": TushareDailyFetchSpec(
        dataset="adj-factor",
        label="复权因子",
        raw_columns=TUSHARE_ADJ_FACTOR_RAW_COLUMNS,
    ),
    "daily-basic": TushareDailyFetchSpec(
        dataset="daily-basic",
        label="每日指标",
        raw_columns=TUSHARE_DAILY_BASIC_RAW_COLUMNS,
    ),
    "stock-st": TushareDailyFetchSpec(
        dataset="stock-st",
        label="ST 股票列表",
        raw_columns=TUSHARE_STOCK_ST_RAW_COLUMNS,
    ),
    "stk-limit": TushareDailyFetchSpec(
        dataset="stk-limit",
        label="涨跌停价格",
        raw_columns=TUSHARE_STK_LIMIT_RAW_COLUMNS,
    ),
    "suspend-d": TushareDailyFetchSpec(
        dataset="suspend-d",
        label="停牌股票列表",
        raw_columns=TUSHARE_SUSPEND_D_RAW_COLUMNS,
    ),
}


TUSHARE_DAILY_LOAD_SPECS: dict[str, TushareDailyLoadSpec] = {
    "daily-ohlcv": TushareDailyLoadSpec(
        dataset="daily-ohlcv",
        label="日线行情",
        processed_dataset="ohlcv",
        processed_columns=DAILY_OHLCV_COLUMNS,
        normalize_frame=_normalize_daily_ohlcv_for_load,
        missing_raw_message="未找到日线行情 raw CSV 文件",
    ),
    "adj-factor": TushareDailyLoadSpec(
        dataset="adj-factor",
        label="复权因子",
        processed_dataset="adj_factor",
        processed_columns=ADJ_FACTOR_COLUMNS,
        normalize_frame=_normalize_adj_factor_for_load,
        missing_raw_message="未找到复权因子 raw CSV 文件",
    ),
    "daily-basic": TushareDailyLoadSpec(
        dataset="daily-basic",
        label="每日指标",
        processed_dataset="daily_basic",
        processed_columns=DAILY_BASIC_COLUMNS,
        normalize_frame=_normalize_daily_basic_for_load,
        missing_raw_message="未找到每日指标 raw CSV 文件",
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
        logger.bind(module="etl").info(
            "开始调用 Tushare 交易日历接口: exchange={}, start_date={}, end_date={}",
            TRADE_CALENDAR_EXCHANGE,
            task.start_date,
            task.end_date,
        )
        _sleep_before_request()
        result = self._pro_api.trade_cal(
            exchange=TRADE_CALENDAR_EXCHANGE,
            start_date=task.start_date.strftime("%Y%m%d"),
            end_date=task.end_date.strftime("%Y%m%d"),
        )
        df = cast(DataFrame, result)
        logger.bind(module="etl").info("Tushare 交易日历接口返回完成: 行数={}", len(df.index))
        return df

    def fetch_daily_frame(self, trade_date: date, spec: TushareDailyFetchSpec) -> DataFrame:
        """调用 Tushare 日频接口并返回单个交易日 DataFrame。"""
        trade_date_text = trade_date.strftime("%Y%m%d")
        _sleep_before_request()
        logger.bind(module="etl").info(
            "开始调用 Tushare {}接口: trade_date={}",
            spec.label,
            trade_date_text,
        )
        try:
            if spec.dataset == "daily-ohlcv":
                result = self._pro_api.daily(trade_date=trade_date_text)
            elif spec.dataset == "adj-factor":
                result = self._pro_api.adj_factor(trade_date=trade_date_text)
            elif spec.dataset == "daily-basic":
                result = self._pro_api.daily_basic(
                    trade_date=trade_date_text,
                    fields=",".join(spec.raw_columns),
                )
            elif spec.dataset == "stock-st":
                result = self._pro_api.stock_st(
                    trade_date=trade_date_text,
                    fields=",".join(spec.raw_columns),
                )
            elif spec.dataset == "stk-limit":
                result = self._pro_api.stk_limit(
                    trade_date=trade_date_text,
                    fields=",".join(spec.raw_columns),
                )
            elif spec.dataset == "suspend-d":
                result = self._pro_api.suspend_d(
                    trade_date=trade_date_text,
                    fields=",".join(spec.raw_columns),
                )
            else:
                raise NotImplementedError(
                    f"暂未实现 Tushare 日频接口: dataset={spec.dataset}"
                )
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

    def fetch_stock_basic(self) -> DataFrame:
        """调用 Tushare 股票基础信息接口。"""
        fields = ",".join(TUSHARE_STOCK_BASIC_RAW_COLUMNS)
        frames: list[DataFrame] = []
        for list_status in STOCK_BASIC_LIST_STATUSES:
            _sleep_before_request()
            logger.bind(module="etl").info(
                "开始调用 Tushare 股票基础信息接口: list_status={}",
                list_status,
            )
            try:
                result = self._pro_api.stock_basic(list_status=list_status, fields=fields)
            except Exception:
                logger.bind(module="etl").exception(
                    "Tushare 股票基础信息接口调用失败: list_status={}",
                    list_status,
                )
                raise

            df = cast(DataFrame, result)
            logger.bind(module="etl").info(
                "Tushare 股票基础信息接口返回完成: list_status={}, 行数={}",
                list_status,
                len(df.index),
            )
            if df.empty:
                frames.append(pd.DataFrame(columns=list(TUSHARE_STOCK_BASIC_RAW_COLUMNS)))
            else:
                frames.append(df)

        if not frames:
            return pd.DataFrame(columns=list(TUSHARE_STOCK_BASIC_RAW_COLUMNS))
        return pd.concat(frames, ignore_index=True)


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
        if task.dataset == "stock-basic":
            return self.fetch_stock_basic(task)
        if task.dataset == "stock-st":
            return self.fetch_stock_st(task)
        if task.dataset == "stk-limit":
            return self.fetch_stk_limit(task)
        if task.dataset == "suspend-d":
            return self.fetch_suspend_d(task)
        raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")

    def load_raw(self, task: ETLTask) -> int:
        """按数据集加载 Tushare raw CSV。"""
        if task.dataset == "trade-calendar":
            return self.load_trade_calendar(task)
        if task.dataset == "daily-ohlcv":
            return self.load_daily_ohlcv(task)
        if task.dataset == "adj-factor":
            return self.load_adj_factor(task)
        if task.dataset == "daily-basic":
            return self.load_daily_basic(task)
        if task.dataset == "stock-basic":
            return self.load_stock_basic(task)
        raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")

    def archive_year(self, dataset: str, year: int) -> Path:
        """按数据集归档 Tushare 日频 processed 数据。"""
        spec = TUSHARE_DAILY_LOAD_SPECS.get(dataset)
        if spec is None:
            raise NotImplementedError(f"暂未实现归档: dataset={dataset}, source=tushare")
        return self._archive_daily_dataset(year, spec)

    def archive_daily_ohlcv_year(self, year: int) -> Path:
        """将某个已结束年份的月度日线 Parquet 归档为年文件。"""
        return self.archive_year("daily-ohlcv", year)

    def archive_adj_factor_year(self, year: int) -> Path:
        """将某个已结束年份的月度复权因子 Parquet 归档为年文件。"""
        return self.archive_year("adj-factor", year)

    def archive_daily_basic_year(self, year: int) -> Path:
        """将某个已结束年份的月度每日指标 Parquet 归档为年文件。"""
        return self.archive_year("daily-basic", year)

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

    def fetch_stock_basic(self, _task: ETLTask) -> DataFrame:
        """拉取 Tushare 股票基础信息快照。"""
        return self._api().fetch_stock_basic()

    def fetch_stock_st(self, task: ETLTask) -> Iterator[tuple[date, DataFrame]]:
        """按本地交易日历逐日拉取 Tushare ST 股票列表原始数据。"""
        return self._fetch_daily_dataset(task, TUSHARE_DAILY_FETCH_SPECS["stock-st"])

    def fetch_stk_limit(self, task: ETLTask) -> Iterator[tuple[date, DataFrame]]:
        """按本地交易日历逐日拉取 Tushare 涨跌停价格原始数据。"""
        return self._fetch_daily_dataset(task, TUSHARE_DAILY_FETCH_SPECS["stk-limit"])

    def fetch_suspend_d(self, task: ETLTask) -> Iterator[tuple[date, DataFrame]]:
        """按本地交易日历逐日拉取 Tushare 停牌股票列表原始数据。"""
        return self._fetch_daily_dataset(task, TUSHARE_DAILY_FETCH_SPECS["suspend-d"])

    def load_trade_calendar(self, task: ETLTask) -> int:
        """读取 Tushare 交易日历 raw CSV, 标准化后写入 DuckDB。"""
        raw_path = build_raw_path(self._config.paths.raw_dir, task)
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
        delete_where = "exchange = ? AND cal_date BETWEEN ? AND ?"
        delete_params: Sequence[Any] = [
            TRADE_CALENDAR_EXCHANGE,
            task.start_date,
            task.end_date,
        ]

        row_count = replace_table_dataframe(
            self._config.paths.database_path,
            self._config.paths.processed_dir,
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

    def load_daily_ohlcv(self, task: ETLTask) -> int:
        """读取 Tushare 日线 raw CSV, 标准化后写入月度 processed Parquet。"""
        return self._load_daily_dataset(task, TUSHARE_DAILY_LOAD_SPECS["daily-ohlcv"])

    def load_adj_factor(self, task: ETLTask) -> int:
        """读取 Tushare 复权因子 raw CSV, 标准化后写入月度 processed Parquet。"""
        return self._load_daily_dataset(task, TUSHARE_DAILY_LOAD_SPECS["adj-factor"])

    def load_daily_basic(self, task: ETLTask) -> int:
        """读取 Tushare 每日指标 raw CSV, 标准化后写入月度 processed Parquet。"""
        return self._load_daily_dataset(task, TUSHARE_DAILY_LOAD_SPECS["daily-basic"])

    def load_stock_basic(self, task: ETLTask) -> int:
        """读取 Tushare 股票基础信息 raw CSV, 全量覆盖写入 DuckDB。"""
        raw_path = build_raw_path(self._config.paths.raw_dir, task)
        if not raw_path.is_file():
            raise FileNotFoundError(f"未找到股票基础信息 raw CSV 文件: {raw_path}")

        logger.bind(module="etl").info("开始加载 Tushare 股票基础信息 raw: 路径={}", raw_path)
        raw_df = read_raw_csv(raw_path)
        missing_columns = [column for column in SECURITY_COLUMNS if column not in raw_df.columns]
        if missing_columns:
            raise ValueError(f"股票基础信息 raw 缺少字段: {missing_columns}")

        security_df = raw_df.loc[:, list(SECURITY_COLUMNS)].copy()
        row_count = replace_table_dataframe(
            self._config.paths.database_path,
            self._config.paths.processed_dir,
            table="dim_security",
            df=security_df,
            columns=SECURITY_COLUMNS,
            delete_where="1 = 1",
            delete_params=[],
        )
        logger.bind(module="etl").info(
            "股票基础信息写入 DuckDB 完成: 表=dim_security, 行数={}",
            row_count,
        )
        return row_count

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
            TRADE_CALENDAR_EXCHANGE,
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

    def _load_daily_dataset(self, task: ETLTask, spec: TushareDailyLoadSpec) -> int:
        """读取日频 raw CSV, 标准化后写入月度 processed Parquet。"""
        raw_files = find_raw_files(self._config.paths.raw_dir, task)
        if not raw_files:
            raise FileNotFoundError(spec.missing_raw_message)

        daily_basic_previous_records = (
            self._load_daily_basic_previous_records(task) if spec.dataset == "daily-basic" else {}
        )

        result = load_daily_raw_csv_to_monthly_parquet(
            raw_files,
            self._config.paths.processed_dir,
            spec.processed_dataset,
            read_frame=read_raw_csv,
            normalize_frame=lambda raw_df, raw_path: self._normalize_daily_frame(
                raw_df,
                task,
                raw_path,
                spec,
                daily_basic_previous_records=daily_basic_previous_records,
            ),
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

    def _normalize_daily_frame(
        self,
        raw_df: DataFrame,
        task: ETLTask,
        raw_path: Path,
        spec: TushareDailyLoadSpec,
        *,
        daily_basic_previous_records: dict[str, dict[str, Any]],
    ) -> DataFrame:
        """按数据集标准化日频 raw DataFrame。"""
        if spec.dataset == "daily-ohlcv":
            stock_st_df = self._read_same_day_raw(raw_path, task, "stock-st")
            stk_limit_df = self._read_same_day_raw(raw_path, task, "stk-limit")
            suspend_d_df = self._read_same_day_raw(raw_path, task, "suspend-d")
            return normalize_daily_ohlcv_df(
                raw_df,
                task,
                stock_st_df=stock_st_df,
                stk_limit_df=stk_limit_df,
                suspend_d_df=suspend_d_df,
            )

        if spec.dataset == "daily-basic":
            suspend_d_df = self._read_same_day_raw(raw_path, task, "suspend-d")
            normalized_df = normalize_daily_basic_df(
                raw_df,
                task,
                suspend_d_df=suspend_d_df,
                previous_records=daily_basic_previous_records,
            )
            self._update_daily_basic_previous_records(
                daily_basic_previous_records,
                normalized_df,
            )
            return normalized_df

        return spec.normalize_frame(raw_df, task, raw_path)

    def _read_same_day_raw(self, raw_path: Path, task: ETLTask, dataset: str) -> DataFrame:
        """读取与当前日频 raw 同交易日的辅助 raw。"""
        trade_date = parse_daily_raw_file_date(raw_path, task)
        if trade_date is None:
            raise ValueError(f"无法从日频 raw 文件名解析交易日: {raw_path}")

        same_day_task = task.model_copy(
            update={
                "dataset": dataset,
                "start_date": trade_date,
                "end_date": trade_date,
            }
        )
        same_day_path = build_raw_path(self._config.paths.raw_dir, same_day_task)
        if not same_day_path.is_file():
            raise FileNotFoundError(
                "缺少日频辅助 raw 文件: "
                f"dataset={dataset}, trade_date={trade_date:%Y-%m-%d}, path={same_day_path}"
            )
        return read_raw_csv(same_day_path)

    def _load_daily_basic_previous_records(self, task: ETLTask) -> dict[str, dict[str, Any]]:
        """读取任务开始日前最后一条每日指标 processed 记录作为补全基准。"""
        dataset_dir = self._config.paths.processed_dir / "daily_basic"
        parquet_files = sorted(dataset_dir.glob("**/*.parquet"))
        if not parquet_files:
            return {}

        frames = []
        for parquet_file in parquet_files:
            df = pd.read_parquet(parquet_file)
            if not df.empty:
                frames.append(df)
        if not frames:
            return {}

        all_df = pd.concat(frames, ignore_index=True)
        all_df["trade_date"] = pd.to_datetime(all_df["trade_date"]).dt.date
        previous_df = all_df.loc[all_df["trade_date"] < task.start_date, list(DAILY_BASIC_COLUMNS)]
        if previous_df.empty:
            return {}

        latest_df = (
            previous_df.sort_values(["ts_code", "trade_date"])
            .drop_duplicates(subset=["ts_code"], keep="last")
            .reset_index(drop=True)
        )
        records = latest_df.to_dict(orient="records")
        return {
            str(row["ts_code"]): {str(key): value for key, value in row.items()}
            for row in records
        }

    @staticmethod
    def _update_daily_basic_previous_records(
        previous_records: dict[str, dict[str, Any]],
        normalized_df: DataFrame,
    ) -> None:
        """用当前已标准化每日指标更新上一开市日缓存。"""
        if normalized_df.empty:
            return

        prepared = normalized_df.loc[:, list(DAILY_BASIC_COLUMNS)].copy()
        prepared["trade_date"] = pd.to_datetime(prepared["trade_date"]).dt.date
        prepared = prepared.sort_values(["ts_code", "trade_date"])
        for row in prepared.to_dict(orient="records"):
            previous_records[str(row["ts_code"])] = {
                str(key): value for key, value in row.items()
            }

    def _archive_daily_dataset(self, year: int, spec: TushareDailyLoadSpec) -> Path:
        """归档 Tushare 日频 processed 月文件为年文件。"""
        year_path = archive_daily_year(
            self._config.paths.processed_dir,
            spec.processed_dataset,
            year,
            key_columns=["ts_code", "trade_date"],
            columns=spec.processed_columns,
        )
        logger.bind(module="etl").info(
            "{}年度归档完成: year={}, 路径={}",
            spec.label,
            year,
            year_path,
        )
        return year_path

    def _api(self) -> _TushareApiClient:
        """懒加载 Tushare API 客户端, 避免离线 load/archive 依赖 token。"""
        if self._api_client is None:
            self._api_client = _TushareApiClient(self._config)
        return self._api_client


def _load_open_trade_dates(config: QuantConfig, task: ETLTask) -> list[date]:
    """从本地交易日历读取任务范围内的开市日。"""
    database_path = config.paths.database_path
    if not database_path.is_file():
        raise ValueError(MISSING_TRADE_CALENDAR_MESSAGE)

    manager = DuckDBManager(database_path, config.paths.processed_dir)
    try:
        with manager.session() as conn:
            repository = QuantRepository(conn)
            trade_dates = repository.get_open_trade_dates(
                task.start_date,
                task.end_date,
                exchange=TRADE_CALENDAR_EXCHANGE,
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
