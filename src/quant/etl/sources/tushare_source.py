"""Tushare 数据源适配器。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import pandas as pd
import tushare as ts
from pandas import DataFrame

from quant.config import QuantConfig
from quant.data.db import DuckDBManager
from quant.etl.etl_model import ETLTask
from quant.etl.fetch import read_raw_csv
from quant.etl.storage import replace_duckdb_dataframe
from quant.utils import build_raw_path


class TushareClient:
    """Tushare API 管理。"""

    def __init__(self, config: QuantConfig) -> None:
        self._tushare_token = config.secrets.tushare_token
        if not self._tushare_token:
            raise ValueError("请在环境变量中设置 LOFTY_QUANT__SECRETS__TUSHARE_TOKEN")
        ts.set_token(self._tushare_token)
        self._pro_api: Any = ts.pro_api()

    def fetch_tushare_raw(self, task: ETLTask) -> DataFrame:
        """按数据集拉取 Tushare 原始 DataFrame。"""
        if task.dataset == "trade-calendar":
            return self.fetch_trade_calendar(task)
        raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")

    def fetch_trade_calendar(self, task: ETLTask) -> DataFrame:
        """拉取交易日历原始数据。"""
        exchange = task.exchange or ""
        result = self._pro_api.trade_cal(
            exchange=exchange,
            start_date=task.start_date.strftime("%Y%m%d"),
            end_date=task.end_date.strftime("%Y%m%d"),
        )
        return cast(DataFrame, result)


def load_tushare_data(config: QuantConfig, task: ETLTask) -> int:
    """按数据集加载 Tushare raw CSV。"""
    if task.dataset == "trade-calendar":
        return load_trade_calendar(config, task)
    raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")


def load_trade_calendar(config: QuantConfig, task: ETLTask) -> int:
    """读取 Tushare 交易日历 raw CSV, 标准化后写入 DuckDB。"""
    raw_path = build_raw_path(config.paths.raw_dir, task)
    if not raw_path.is_file():
        raise FileNotFoundError(f"未找到 raw CSV 文件: {raw_path}")

    raw_df = read_raw_csv(raw_path)
    calendar_df = normalize_trade_calendar_df(raw_df, task)

    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
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
    return row_count


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


def _require_columns(df: DataFrame, columns: Sequence[str]) -> None:
    """校验 raw DataFrame 必须包含指定字段。"""
    missing_columns = [column for column in columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"交易日历 raw 缺少字段: {missing_columns}")


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


def _missing_string_mask(series: pd.Series) -> pd.Series:
    """判断字符串 Series 中的空值。"""
    return series.isna() | series.str.lower().isin({"", "nan", "nat", "none", "<na>"})
