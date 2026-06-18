"""日频 processed Parquet 写入和归档工具。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import cast

import pandas as pd
from pandas import DataFrame


def build_daily_month_path(
    processed_dir: Path,
    dataset: str,
    year: int,
    month: int,
    *,
    filename_prefix: str | None = None,
) -> Path:
    """生成日频月度 processed Parquet 路径。"""
    prefix = filename_prefix or dataset
    return (
        processed_dir.expanduser().resolve()
        / dataset
        / f"year={year}"
        / f"month={month:02d}"
        / f"{prefix}_{year}{month:02d}.parquet"
    )


def write_daily_month_parquet(
    processed_dir: Path,
    dataset: str,
    df: DataFrame,
    *,
    date_column: str,
    key_columns: Sequence[str],
    columns: Sequence[str],
    filename_prefix: str | None = None,
) -> dict[Path, int]:
    """按交易日所在年月写入日频月度 Parquet。"""
    if df.empty:
        return {}

    prepared_df = _prepare_daily_frame(
        df,
        date_column=date_column,
        key_columns=key_columns,
        columns=columns,
    )
    month_frame = prepared_df.assign(
        _year=prepared_df[date_column].map(lambda value: value.year),
        _month=prepared_df[date_column].map(lambda value: value.month),
    )

    written: dict[Path, int] = {}
    for group_key, grouped_df in month_frame.groupby(["_year", "_month"], sort=True):
        year, month = cast(tuple[int, int], group_key)
        month_df = grouped_df.drop(columns=["_year", "_month"])
        output_path = build_daily_month_path(
            processed_dir,
            dataset,
            year,
            month,
            filename_prefix=filename_prefix,
        )
        merged_df = _merge_daily_parquet(
            output_path,
            month_df,
            key_columns=key_columns,
            columns=columns,
            date_column=date_column,
        )
        _write_parquet_atomic(output_path, merged_df)
        written[output_path] = len(month_df.index)
    return written


def archive_daily_year(
    processed_dir: Path,
    dataset: str,
    year: int,
    *,
    key_columns: Sequence[str],
    columns: Sequence[str],
    filename_prefix: str | None = None,
) -> Path:
    """将某个已结束年份的日频月文件归档为年文件。"""
    if year >= date.today().year:
        raise ValueError("只能归档已结束年份")

    prefix = filename_prefix or dataset
    year_dir = processed_dir.expanduser().resolve() / dataset / f"year={year}"
    month_files = sorted(year_dir.glob(f"month=*/{prefix}_{year}[0-1][0-9].parquet"))
    if not month_files:
        raise FileNotFoundError("未找到可归档的月度日频文件")

    year_path = year_dir / f"{prefix}_{year}.parquet"
    frames: list[DataFrame] = []
    if year_path.is_file():
        frames.append(_read_parquet(year_path))
    frames.extend(_read_parquet(path) for path in month_files)

    archived_df = _deduplicate_daily_frame(
        pd.concat(frames, ignore_index=True),
        key_columns=key_columns,
        columns=columns,
    )
    _write_parquet_atomic(year_path, archived_df)
    for month_file in month_files:
        month_file.unlink()
    return year_path


def _prepare_daily_frame(
    df: DataFrame,
    *,
    date_column: str,
    key_columns: Sequence[str],
    columns: Sequence[str],
) -> DataFrame:
    """校验字段并将日期字段标准化为 date。"""
    _require_output_columns(date_column=date_column, key_columns=key_columns, columns=columns)
    _require_columns(df, [date_column, *key_columns, *columns])
    prepared_df = df.loc[:, list(columns)].copy()
    prepared_df[date_column] = pd.to_datetime(prepared_df[date_column]).dt.date
    return prepared_df


def _merge_daily_parquet(
    output_path: Path,
    new_df: DataFrame,
    *,
    key_columns: Sequence[str],
    columns: Sequence[str],
    date_column: str,
) -> DataFrame:
    """合并旧月文件和新数据。"""
    frames = []
    if output_path.is_file():
        frames.append(_read_parquet(output_path))
    frames.append(new_df)
    return _deduplicate_daily_frame(
        pd.concat(frames, ignore_index=True),
        key_columns=key_columns,
        columns=columns,
        date_column=date_column,
    )


def _deduplicate_daily_frame(
    df: DataFrame,
    *,
    key_columns: Sequence[str],
    columns: Sequence[str],
    date_column: str = "trade_date",
) -> DataFrame:
    """按 key_columns 去重, 后出现的数据覆盖先出现的数据。"""
    _require_output_columns(date_column=date_column, key_columns=key_columns, columns=columns)
    if df.empty:
        return pd.DataFrame(columns=list(columns))

    output = df.loc[:, list(columns)].copy()
    output[date_column] = pd.to_datetime(output[date_column]).dt.date
    output = output.drop_duplicates(subset=list(key_columns), keep="last")
    sort_columns = list(dict.fromkeys([date_column, *key_columns]))
    return output.sort_values(sort_columns).reset_index(drop=True)


def _read_parquet(path: Path) -> DataFrame:
    """读取 Parquet 为 DataFrame。"""
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


def _require_columns(df: DataFrame, columns: Sequence[str]) -> None:
    """校验 DataFrame 必须包含指定字段。"""
    missing_columns = [column for column in columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"日频 processed 数据缺少字段: {missing_columns}")


def _require_output_columns(
    *,
    date_column: str,
    key_columns: Sequence[str],
    columns: Sequence[str],
) -> None:
    """校验输出字段必须包含日期和主键字段。"""
    missing_columns = [column for column in [date_column, *key_columns] if column not in columns]
    if missing_columns:
        raise ValueError(f"日频 processed 输出字段缺少关键字段: {missing_columns}")
