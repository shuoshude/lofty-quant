"""因子研究质量评价。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from itertools import pairwise
from math import isfinite
from typing import cast

import polars as pl

from quant.config import QuantConfig
from quant.data.db import DuckDBManager
from quant.data.repository import QuantRepository
from quant.features.base import FactorMetadata
from quant.features.labels import compute_forward_return_5d
from quant.features.registry import build_default_registry

MIN_CROSS_SECTION_SIZE = 5
QUANTILE_COUNT = 5
FACTOR_COLUMNS = ("ts_code", "trade_date", "factor_name", "factor_value", "factor_version")
LABEL_COLUMNS = ("ts_code", "trade_date", "forward_return_5d")
UNIVERSE_COLUMNS = ("ts_code", "trade_date")
KEY_COLUMNS = ("ts_code", "trade_date")


@dataclass(frozen=True, slots=True)
class FactorDateCoverage:
    """描述单个交易日的有效因子覆盖情况。"""

    trade_date: date
    universe_count: int
    valid_count: int
    coverage: float


@dataclass(frozen=True, slots=True)
class FactorValidationReport:
    """汇总一个因子在固定区间内的研究质量。"""

    factor_name: str
    factor_version: str
    start_date: date
    end_date: date
    row_count: int
    trade_date_count: int
    security_count: int
    missing_value_rate: float
    coverage_by_date: tuple[FactorDateCoverage, ...]
    mean: float | None
    std: float | None
    min: float | None
    p25: float | None
    median: float | None
    p75: float | None
    max: float | None
    ic_5d_mean: float | None
    rank_ic_5d_mean: float | None
    rank_ic_5d_std: float | None
    rank_ic_5d_count: int
    ic_ir: float | None
    positive_ic_ratio: float | None
    q1_return: float | None
    q2_return: float | None
    q3_return: float | None
    q4_return: float | None
    q5_return: float | None
    long_short_return: float | None
    turnover: float | None
    factor_autocorr_1d: float | None


def run_factor_validation(
    config: QuantConfig,
    factor_name: str,
    start_date: date,
    end_date: date,
    *,
    factor_version: str = "v1",
) -> FactorValidationReport:
    """读取已存因子和 HFQ 开盘价并完成五日未来收益评价。"""
    if start_date > end_date:
        raise ValueError("start_date 不能晚于 end_date")

    metadata = build_default_registry().get(factor_name, factor_version)
    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    try:
        manager.initialize()
        with manager.session() as conn:
            repository = QuantRepository(conn)
            factors = repository.get_factor_panel(
                start_date,
                end_date,
                factor_name,
                factor_version=factor_version,
            )
            if factors.is_empty():
                raise ValueError(
                    "因子评价输入为空: "
                    f"name={factor_name}, version={factor_version}, "
                    f"start_date={start_date}, end_date={end_date}"
                )

            available_trade_dates = repository.get_open_trade_dates(start_date, date.max)
            requested_indices = [
                index
                for index, trade_date in enumerate(available_trade_dates)
                if trade_date <= end_date
            ]
            if not requested_indices:
                raise ValueError(
                    f"请求区间没有 SSE 交易日: start_date={start_date}, end_date={end_date}"
                )
            last_calendar_index = min(requested_indices[-1] + 6, len(available_trade_dates) - 1)
            trade_dates = available_trade_dates[: last_calendar_index + 1]
            price_panel = repository.get_daily_panel(
                start_date,
                trade_dates[-1],
                ["hfq_open"],
                adjustment="hfq",
            )

        if price_panel.is_empty():
            raise ValueError(f"因子评价行情面板为空: start_date={start_date}, end_date={end_date}")
        labels = compute_forward_return_5d(price_panel, trade_dates)
        universe = price_panel.filter(
            pl.col("trade_date").is_between(start_date, end_date, closed="both")
        ).select(*KEY_COLUMNS)
        return validate_factor(factors, labels, universe, metadata)
    finally:
        manager.close()


def validate_factor(
    factors: pl.DataFrame,
    labels: pl.DataFrame,
    universe: pl.DataFrame,
    metadata: FactorMetadata,
) -> FactorValidationReport:
    """根据未来五日收益评价一个 long format 因子面板。"""
    _validate_inputs(factors, labels, universe, metadata)

    clean_factors = factors.with_columns(
        pl.when(pl.col("factor_value").is_not_null() & pl.col("factor_value").is_finite())
        .then(pl.col("factor_value"))
        .otherwise(None)
        .alias("_factor_value")
    )
    clean_labels = labels.with_columns(
        pl.when(pl.col("forward_return_5d").is_not_null() & pl.col("forward_return_5d").is_finite())
        .then(pl.col("forward_return_5d"))
        .otherwise(None)
        .alias("_forward_return_5d")
    )
    valid_values = clean_factors["_factor_value"].drop_nulls()
    start_date = cast(date, universe["trade_date"].min())
    end_date = cast(date, universe["trade_date"].max())

    coverage = _calculate_coverage(clean_factors, universe)
    evaluation = (
        clean_factors.select(*KEY_COLUMNS, "_factor_value")
        .join(clean_labels.select(*KEY_COLUMNS, "_forward_return_5d"), on=KEY_COLUMNS)
        .drop_nulls(["_factor_value", "_forward_return_5d"])
        .sort(["trade_date", "ts_code"])
    )
    daily_metrics = _calculate_daily_metrics(evaluation, metadata)

    rank_ic_std = _sample_std(daily_metrics.rank_ics)
    rank_ic_mean = _mean(daily_metrics.rank_ics)
    ic_ir = None
    if rank_ic_mean is not None and rank_ic_std is not None and rank_ic_std != 0.0:
        ic_ir = rank_ic_mean / rank_ic_std

    quantile_returns = tuple(_mean(values) for values in daily_metrics.quantile_returns)
    return FactorValidationReport(
        factor_name=metadata.name,
        factor_version=metadata.version,
        start_date=start_date,
        end_date=end_date,
        row_count=factors.height,
        trade_date_count=factors["trade_date"].n_unique(),
        security_count=factors["ts_code"].n_unique(),
        missing_value_rate=1 - valid_values.len() / factors.height,
        coverage_by_date=coverage,
        mean=_series_stat(valid_values, "mean"),
        std=_series_stat(valid_values, "std"),
        min=_series_stat(valid_values, "min"),
        p25=_quantile(valid_values, 0.25),
        median=_quantile(valid_values, 0.5),
        p75=_quantile(valid_values, 0.75),
        max=_series_stat(valid_values, "max"),
        ic_5d_mean=_mean(daily_metrics.pearson_ics),
        rank_ic_5d_mean=rank_ic_mean,
        rank_ic_5d_std=rank_ic_std,
        rank_ic_5d_count=len(daily_metrics.rank_ics),
        ic_ir=ic_ir,
        positive_ic_ratio=(
            sum(value > 0 for value in daily_metrics.rank_ics) / len(daily_metrics.rank_ics)
            if daily_metrics.rank_ics
            else None
        ),
        q1_return=quantile_returns[0],
        q2_return=quantile_returns[1],
        q3_return=quantile_returns[2],
        q4_return=quantile_returns[3],
        q5_return=quantile_returns[4],
        long_short_return=_mean(daily_metrics.long_short_returns),
        turnover=_calculate_turnover(daily_metrics.favored_groups),
        factor_autocorr_1d=_calculate_factor_autocorrelation(clean_factors),
    )


@dataclass(frozen=True, slots=True)
class _DailyMetrics:
    pearson_ics: tuple[float, ...]
    rank_ics: tuple[float, ...]
    quantile_returns: tuple[tuple[float, ...], ...]
    long_short_returns: tuple[float, ...]
    favored_groups: tuple[tuple[date, frozenset[str]], ...]


def _validate_inputs(
    factors: pl.DataFrame,
    labels: pl.DataFrame,
    universe: pl.DataFrame,
    metadata: FactorMetadata,
) -> None:
    """校验评价输入的列、身份和唯一键。"""
    if factors.is_empty():
        raise ValueError("因子评价输入不能为空")
    _require_columns(factors, FACTOR_COLUMNS, "因子评价输入")
    _require_columns(labels, LABEL_COLUMNS, "未来收益标签")
    _require_columns(universe, UNIVERSE_COLUMNS, "股票池")
    if universe.is_empty():
        raise ValueError("股票池不能为空")
    _reject_duplicate_keys(factors, KEY_COLUMNS, "因子评价输入")
    _reject_duplicate_keys(labels, KEY_COLUMNS, "未来收益标签")
    _reject_duplicate_keys(universe, KEY_COLUMNS, "股票池")

    identities = set(factors.select("factor_name", "factor_version").unique().iter_rows())
    if identities != {(metadata.name, metadata.version)}:
        raise ValueError(f"因子评价身份不匹配: expected={metadata.name}:{metadata.version}")

    factor_dates = set(factors["trade_date"].to_list())
    universe_dates = set(universe["trade_date"].to_list())
    label_dates = set(labels["trade_date"].to_list())
    if not factor_dates.issubset(universe_dates):
        raise ValueError("因子日期不在股票池日期范围内")
    if not factor_dates.issubset(label_dates):
        raise ValueError("因子日期不在未来收益标签日期范围内")


def _require_columns(frame: pl.DataFrame, columns: tuple[str, ...], label: str) -> None:
    """要求 DataFrame 包含指定字段。"""
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{label}缺少字段: {missing}")


def _reject_duplicate_keys(
    frame: pl.DataFrame,
    key_columns: tuple[str, ...],
    label: str,
) -> None:
    """拒绝无法一对一连接的重复键。"""
    if frame.select(key_columns).is_duplicated().any():
        raise ValueError(f"{label}包含重复键: key_columns={key_columns}")


def _calculate_coverage(
    factors: pl.DataFrame,
    universe: pl.DataFrame,
) -> tuple[FactorDateCoverage, ...]:
    """按股票池实际行数计算每日有效因子覆盖率。"""
    rows = (
        universe.select(*KEY_COLUMNS)
        .join(factors.select(*KEY_COLUMNS, "_factor_value"), on=KEY_COLUMNS, how="left")
        .group_by("trade_date")
        .agg(
            pl.len().alias("universe_count"),
            pl.col("_factor_value").count().alias("valid_count"),
        )
        .sort("trade_date")
        .to_dicts()
    )
    return tuple(
        FactorDateCoverage(
            trade_date=cast(date, row["trade_date"]),
            universe_count=int(row["universe_count"]),
            valid_count=int(row["valid_count"]),
            coverage=float(row["valid_count"]) / float(row["universe_count"]),
        )
        for row in rows
    )


def _calculate_daily_metrics(
    evaluation: pl.DataFrame,
    metadata: FactorMetadata,
) -> _DailyMetrics:
    """计算有效截面的 IC、分组收益和方向组合成员。"""
    pearson_ics: list[float] = []
    rank_ics: list[float] = []
    quantile_returns: list[list[float]] = [[] for _index in range(QUANTILE_COUNT)]
    long_short_returns: list[float] = []
    favored_groups: list[tuple[date, frozenset[str]]] = []

    for frame in evaluation.partition_by("trade_date", maintain_order=True):
        if frame.height < MIN_CROSS_SECTION_SIZE:
            continue
        if frame["_factor_value"].n_unique() < 2 or frame["_forward_return_5d"].n_unique() < 2:
            continue

        pearson = _correlation(frame, "_factor_value", "_forward_return_5d")
        ranked = frame.with_columns(
            pl.col("_factor_value").rank(method="average").alias("_factor_rank"),
            pl.col("_forward_return_5d").rank(method="average").alias("_return_rank"),
        )
        rank_ic = _correlation(ranked, "_factor_rank", "_return_rank")
        if pearson is None or rank_ic is None:
            continue
        pearson_ics.append(pearson)
        rank_ics.append(rank_ic)

        grouped = (
            frame.sort(["_factor_value", "ts_code"])
            .with_row_index("_position")
            .with_columns(
                ((pl.col("_position") * QUANTILE_COUNT // frame.height) + 1)
                .cast(pl.Int8)
                .alias("_quantile")
            )
        )
        group_returns = {
            int(row["_quantile"]): float(row["group_return"])
            for row in grouped.group_by("_quantile")
            .agg(pl.col("_forward_return_5d").mean().alias("group_return"))
            .to_dicts()
        }
        if len(group_returns) != QUANTILE_COUNT:
            continue
        for quantile in range(1, QUANTILE_COUNT + 1):
            quantile_returns[quantile - 1].append(group_returns[quantile])

        low_return = group_returns[1]
        high_return = group_returns[QUANTILE_COUNT]
        if metadata.higher_is_better is True:
            long_short_returns.append(high_return - low_return)
            favored_quantile = QUANTILE_COUNT
        elif metadata.higher_is_better is False:
            long_short_returns.append(low_return - high_return)
            favored_quantile = 1
        else:
            favored_quantile = 0
        if favored_quantile:
            trade_date = cast(date, frame["trade_date"][0])
            members = frozenset(
                grouped.filter(pl.col("_quantile") == favored_quantile)["ts_code"].to_list()
            )
            favored_groups.append((trade_date, members))

    return _DailyMetrics(
        pearson_ics=tuple(pearson_ics),
        rank_ics=tuple(rank_ics),
        quantile_returns=tuple(tuple(values) for values in quantile_returns),
        long_short_returns=tuple(long_short_returns),
        favored_groups=tuple(favored_groups),
    )


def _calculate_turnover(
    favored_groups: tuple[tuple[date, frozenset[str]], ...],
) -> float | None:
    """计算相邻有效日期方向最优组的等权换手率。"""
    if len(favored_groups) < 2:
        return None
    values: list[float] = []
    for (_previous_date, previous), (_current_date, current) in pairwise(favored_groups):
        previous_weight = 1 / len(previous)
        current_weight = 1 / len(current)
        values.append(
            0.5
            * sum(
                abs(
                    (current_weight if ts_code in current else 0.0)
                    - (previous_weight if ts_code in previous else 0.0)
                )
                for ts_code in previous | current
            )
        )
    return _mean(tuple(values))


def _calculate_factor_autocorrelation(factors: pl.DataFrame) -> float | None:
    """计算相邻因子日期共同证券的截面 Pearson 相关均值。"""
    ordered = factors.select(*KEY_COLUMNS, "_factor_value").sort(["trade_date", "ts_code"])
    frames = ordered.partition_by("trade_date", maintain_order=True)
    correlations: list[float] = []
    for previous, current in pairwise(frames):
        paired = (
            previous.drop_nulls("_factor_value")
            .select("ts_code", pl.col("_factor_value").alias("_previous"))
            .join(
                current.drop_nulls("_factor_value").select(
                    "ts_code", pl.col("_factor_value").alias("_current")
                ),
                on="ts_code",
            )
        )
        if (
            paired.height < MIN_CROSS_SECTION_SIZE
            or paired["_previous"].n_unique() < 2
            or paired["_current"].n_unique() < 2
        ):
            continue
        correlation = _correlation(paired, "_previous", "_current")
        if correlation is not None:
            correlations.append(correlation)
    return _mean(tuple(correlations))


def _correlation(frame: pl.DataFrame, left: str, right: str) -> float | None:
    """返回有限 Pearson 相关系数。"""
    value = frame.select(pl.corr(left, right)).item()
    if value is None or not isfinite(float(value)):
        return None
    return float(value)


def _series_stat(series: pl.Series, statistic: str) -> float | None:
    """返回空序列安全的基础统计值。"""
    if series.is_empty():
        return None
    value = getattr(series, statistic)()
    return float(value) if value is not None and isfinite(float(value)) else None


def _quantile(series: pl.Series, quantile: float) -> float | None:
    """返回线性插值分位数。"""
    if series.is_empty():
        return None
    value = series.quantile(quantile, interpolation="linear")
    return float(value) if value is not None and isfinite(float(value)) else None


def _mean(values: tuple[float, ...]) -> float | None:
    """返回空集合安全的均值。"""
    return sum(values) / len(values) if values else None


def _sample_std(values: tuple[float, ...]) -> float | None:
    """返回至少两个观测的样本标准差。"""
    if len(values) < 2:
        return None
    value = cast(float | None, pl.Series(values, dtype=pl.Float64).std())
    return float(value) if value is not None and isfinite(float(value)) else None
