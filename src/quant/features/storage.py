"""因子结果的月度 Parquet 存储。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas import DataFrame

from quant.data.fields import FACTOR_COLUMNS
from quant.etl.processed import write_daily_month_parquet

FACTOR_OPTIONAL_COLUMN_DTYPES = (
    ("raw_value", "float64"),
    ("processed_value", "float64"),
    ("quality_status", "string"),
    ("created_at", "datetime64[ns]"),
)
FACTOR_OPTIONAL_COLUMNS = tuple(column for column, _dtype in FACTOR_OPTIONAL_COLUMN_DTYPES)
FACTOR_STORAGE_COLUMNS = (*FACTOR_COLUMNS, *FACTOR_OPTIONAL_COLUMNS)
FACTOR_KEY_COLUMNS = ("ts_code", "trade_date", "factor_name", "factor_version")


def write_factor_results(processed_dir: Path, df: DataFrame) -> dict[Path, int]:
    """将标准因子结果按交易月份写入 processed/factors。"""
    if df.empty:
        return {}

    missing_columns = [column for column in FACTOR_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"因子结果缺少字段: {missing_columns}")

    unsupported_columns = [column for column in df.columns if column not in FACTOR_STORAGE_COLUMNS]
    if unsupported_columns:
        raise ValueError(f"因子结果包含不支持的字段: {unsupported_columns}")

    prepared_df = df.copy()
    for column, dtype in FACTOR_OPTIONAL_COLUMN_DTYPES:
        if column not in prepared_df.columns:
            prepared_df[column] = pd.Series(index=prepared_df.index, dtype=dtype)

    return write_daily_month_parquet(
        processed_dir,
        "factors",
        prepared_df,
        date_column="trade_date",
        key_columns=FACTOR_KEY_COLUMNS,
        columns=FACTOR_STORAGE_COLUMNS,
    )
