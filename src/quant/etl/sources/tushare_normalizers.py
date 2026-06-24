"""Tushare raw DataFrame 标准化工具。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd
from loguru import logger
from pandas import DataFrame
from pydantic import ValidationError

from quant.data.fields import (
    ADJ_FACTOR_COLUMNS,
    DAILY_BASIC_COLUMNS,
    DAILY_OHLCV_COLUMNS,
    TUSHARE_ADJ_FACTOR_REQUIRED_COLUMNS,
    TUSHARE_DAILY_BASIC_REQUIRED_COLUMNS,
    TUSHARE_DAILY_OHLCV_REQUIRED_COLUMNS,
)
from quant.data.schemas import AdjFactorRecord, DailyBasicRecord, DailyOHLCVRecord
from quant.etl.etl_model import ETLTask


def normalize_trade_calendar_df(raw_df: DataFrame, task: ETLTask) -> DataFrame:
    """将 Tushare 交易日历 raw DataFrame 向量化转换为项目标准表结构。"""
    _require_columns(raw_df, ["cal_date", "is_open"])
    default_exchange = "SSE"

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
