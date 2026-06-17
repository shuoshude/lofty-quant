"""Tushare 数据源适配器。"""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Iterator, Sequence
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
from quant.data.repository import QuantRepository
from quant.etl.etl_model import ETLTask
from quant.etl.fetch import find_raw_files, read_raw_csv
from quant.etl.storage import replace_duckdb_dataframe
from quant.utils import build_raw_path

TUSHARE_REQUEST_SLEEP_SECONDS = 0.2
DAILY_OHLCV_RAW_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
)
DAILY_OHLCV_REQUIRED_RAW_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
)
DAILY_OHLCV_PROCESSED_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "volume",
    "amount",
    "is_suspended",
    "is_st",
    "limit_status",
)
MISSING_TRADE_CALENDAR_MESSAGE = "请先加载交易日历后再拉取日线行情"


class TushareClient:
    """Tushare API 管理。"""

    def __init__(self, config: QuantConfig) -> None:
        self._config = config
        self._tushare_token = config.secrets.tushare_token
        if not self._tushare_token:
            raise ValueError("请在环境变量中设置 LOFTY_QUANT__SECRETS__TUSHARE_TOKEN")
        ts.set_token(self._tushare_token)
        self._pro_api: Any = ts.pro_api()

    def fetch_tushare_raw(self, task: ETLTask) -> DataFrame | Iterator[tuple[date, DataFrame]]:
        """按数据集拉取 Tushare 原始 DataFrame。"""
        if task.dataset == "trade-calendar":
            return self.fetch_trade_calendar(task)
        if task.dataset == "daily-ohlcv":
            return self.fetch_daily_ohlcv(task)
        raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")

    def fetch_trade_calendar(self, task: ETLTask) -> DataFrame:
        """拉取交易日历原始数据。"""
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

    def fetch_daily_ohlcv(self, task: ETLTask) -> Iterator[tuple[date, DataFrame]]:
        """按本地交易日历逐日拉取 Tushare 日线行情原始数据。"""
        trade_dates = _load_open_trade_dates(self._config, task)
        logger.bind(module="etl").info(
            "开始拉取 Tushare 日线行情: exchange={}, 开市日数量={}",
            task.exchange or "SSE",
            len(trade_dates),
        )

        for trade_date in trade_dates:
            trade_date_text = trade_date.strftime("%Y%m%d")
            _sleep_before_request()
            logger.bind(module="etl").info(
                "开始调用 Tushare 日线行情接口: trade_date={}",
                trade_date_text,
            )
            try:
                result = self._pro_api.daily(trade_date=trade_date_text)
            except Exception:
                logger.bind(module="etl").exception(
                    "Tushare 日线行情接口调用失败: trade_date={}",
                    trade_date_text,
                )
                raise

            df = cast(DataFrame, result)
            logger.bind(module="etl").info(
                "Tushare 日线行情接口返回完成: trade_date={}, 行数={}",
                trade_date_text,
                len(df.index),
            )
            if df.empty:
                logger.bind(module="etl").info(
                    "Tushare 日线行情返回为空, 写出空 raw CSV 表头: trade_date={}",
                    trade_date_text,
                )
                yield trade_date, pd.DataFrame(columns=DAILY_OHLCV_RAW_COLUMNS)
                continue

            yield trade_date, df

        logger.bind(module="etl").info(
            "Tushare 日线行情按日拉取完成: 交易日数量={}",
            len(trade_dates),
        )


def load_tushare_data(config: QuantConfig, task: ETLTask) -> int:
    """按数据集加载 Tushare raw CSV。"""
    if task.dataset == "trade-calendar":
        return load_trade_calendar(config, task)
    if task.dataset == "daily-ohlcv":
        return load_daily_ohlcv(config, task)
    raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")


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

    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
        # 交易日历以目标表为事实源, 同一范围重跑时直接覆盖旧数据。
        if task.exchange:
            delete_where = "exchange = ? AND cal_date BETWEEN ? AND ?"
            delete_params: Sequence[Any] = [task.exchange.upper(), task.start_date, task.end_date]
        else:
            delete_where = "cal_date BETWEEN ? AND ?"
            delete_params = [task.start_date, task.end_date]

        row_count = replace_duckdb_dataframe(
            conn,
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
    raw_files = find_raw_files(config.paths.raw_dir, task)
    if not raw_files:
        raise FileNotFoundError("未找到日线行情 raw CSV 文件")

    monthly_frames: defaultdict[tuple[int, int], list[DataFrame]] = defaultdict(list)
    row_count = 0
    for raw_path in raw_files:
        logger.bind(module="etl").info("开始加载 Tushare 日线 raw: 路径={}", raw_path)
        raw_df = read_raw_csv(raw_path)
        normalized_df = normalize_daily_ohlcv_df(raw_df, task)
        if normalized_df.empty:
            logger.bind(module="etl").info("日线 raw 为空, 跳过 processed 写入: 路径={}", raw_path)
            continue

        row_count += len(normalized_df.index)
        month_frame = normalized_df.assign(
            _year=normalized_df["trade_date"].map(lambda value: value.year),
            _month=normalized_df["trade_date"].map(lambda value: value.month),
        )
        for group_key, month_df in month_frame.groupby(["_year", "_month"], sort=True):
            year, month = cast(tuple[int, int], group_key)
            monthly_frames[(year, month)].append(
                month_df.drop(columns=["_year", "_month"]),
            )

    if task.dry_run:
        logger.bind(module="etl").info("试运行: 跳过日线 processed 写入, 行数={}", row_count)
        return row_count

    for (year, month), frames in sorted(monthly_frames.items()):
        month_df = pd.concat(frames, ignore_index=True)
        output_path = _daily_ohlcv_month_path(config.paths.processed_dir, year, month)
        _merge_write_daily_ohlcv_parquet(output_path, month_df)
        logger.bind(module="etl").info(
            "日线行情 processed 月文件写入完成: 路径={}, 新增行数={}",
            output_path,
            len(month_df.index),
        )

    return row_count


def archive_daily_ohlcv_year(config: QuantConfig, year: int) -> Path:
    """将某个已结束年份的月度日线 Parquet 归档为年文件。"""
    current_year = date.today().year
    if year >= current_year:
        raise ValueError("只能归档已结束年份")

    year_dir = config.paths.processed_dir.expanduser().resolve() / "ohlcv" / f"year={year}"
    month_files = sorted(year_dir.glob(f"month=*/ohlcv_{year}[0-1][0-9].parquet"))
    if not month_files:
        raise FileNotFoundError("未找到可归档的月度日线文件")

    year_path = year_dir / f"ohlcv_{year}.parquet"
    frames: list[DataFrame] = []
    if year_path.is_file():
        frames.append(_read_processed_parquet(year_path))
    frames.extend(_read_processed_parquet(path) for path in month_files)

    archived_df = _deduplicate_daily_ohlcv(pd.concat(frames, ignore_index=True))
    _write_parquet_atomic(year_path, archived_df)
    for month_file in month_files:
        month_file.unlink()

    logger.bind(module="etl").info(
        "日线行情年度归档完成: year={}, 路径={}, 行数={}",
        year,
        year_path,
        len(archived_df.index),
    )
    return year_path


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
        return pd.DataFrame(columns=DAILY_OHLCV_PROCESSED_COLUMNS)

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
    return output.loc[:, list(DAILY_OHLCV_PROCESSED_COLUMNS)]


def _require_columns(df: DataFrame, columns: Sequence[str]) -> None:
    """校验 raw DataFrame 必须包含指定字段。"""
    missing_columns = [column for column in columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"交易日历 raw 缺少字段: {missing_columns}")


def _require_daily_ohlcv_columns(df: DataFrame) -> None:
    """校验日线行情 raw DataFrame 必须包含核心字段。"""
    missing_columns = [
        column for column in DAILY_OHLCV_REQUIRED_RAW_COLUMNS if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"日线行情 raw 缺少字段: {missing_columns}")


def _validate_daily_ohlcv_date_range(series: pd.Series, task: ETLTask) -> None:
    """校验 raw 中的交易日是否落在任务范围内。"""
    invalid_mask = (series < task.start_date) | (series > task.end_date)
    if invalid_mask.any():
        invalid_values = series[invalid_mask].head(3).tolist()
        raise ValueError(f"日线行情 raw 日期超出任务范围: {invalid_values}")


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


def _daily_ohlcv_month_path(processed_dir: Path, year: int, month: int) -> Path:
    """生成日线行情月度 processed Parquet 路径。"""
    return (
        processed_dir.expanduser().resolve()
        / "ohlcv"
        / f"year={year}"
        / f"month={month:02d}"
        / f"ohlcv_{year}{month:02d}.parquet"
    )


def _merge_write_daily_ohlcv_parquet(output_path: Path, new_df: DataFrame) -> None:
    """合并旧月文件和新数据后覆盖写入。"""
    frames = []
    if output_path.is_file():
        frames.append(_read_processed_parquet(output_path))
    frames.append(new_df)

    merged_df = _deduplicate_daily_ohlcv(pd.concat(frames, ignore_index=True))
    _write_parquet_atomic(output_path, merged_df)


def _deduplicate_daily_ohlcv(df: DataFrame) -> DataFrame:
    """按 ts_code 和 trade_date 去重, 后出现的数据覆盖先出现的数据。"""
    if df.empty:
        return pd.DataFrame(columns=DAILY_OHLCV_PROCESSED_COLUMNS)

    output = df.loc[:, list(DAILY_OHLCV_PROCESSED_COLUMNS)].copy()
    output["trade_date"] = pd.to_datetime(output["trade_date"]).dt.date
    output = output.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    return output.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def _read_processed_parquet(path: Path) -> DataFrame:
    """读取 processed Parquet 为 DataFrame。"""
    return pd.read_parquet(path)


def _write_parquet_atomic(path: Path, df: DataFrame) -> None:
    """通过临时文件替换的方式写入 Parquet。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.stem}.tmp{path.suffix}")
    try:
        df.to_parquet(temporary_path, index=False)
        temporary_path.replace(path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


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


def _sleep_before_request() -> None:
    """Tushare 请求之间固定等待, 避免超过每分钟 500 次。"""
    time.sleep(TUSHARE_REQUEST_SLEEP_SECONDS)
