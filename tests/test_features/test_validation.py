from dataclasses import replace
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from quant.config import QuantConfig, load_config
from quant.data.db import DuckDBManager
from quant.features import (
    build_default_registry,
    run_factor_validation,
    validate_factor,
    write_factor_results,
)


def make_factors(
    trade_dates: list[date],
    values_by_date: list[list[float | int | None]],
) -> pl.DataFrame:
    """构造 long format 因子面板。"""
    rows = [
        (f"{index + 1:06d}.SZ", trade_date, value)
        for trade_date, values in zip(trade_dates, values_by_date, strict=True)
        for index, value in enumerate(values)
    ]
    return pl.DataFrame(
        {
            "ts_code": [row[0] for row in rows],
            "trade_date": [row[1] for row in rows],
            "factor_name": ["return_5d"] * len(rows),
            "factor_value": [row[2] for row in rows],
            "factor_version": ["v1"] * len(rows),
        }
    )


def make_labels(
    trade_dates: list[date],
    values_by_date: list[list[float | None]],
) -> pl.DataFrame:
    """构造五日未来收益标签。"""
    rows = [
        (f"{index + 1:06d}.SZ", trade_date, value)
        for trade_date, values in zip(trade_dates, values_by_date, strict=True)
        for index, value in enumerate(values)
    ]
    return pl.DataFrame(
        {
            "ts_code": [row[0] for row in rows],
            "trade_date": [row[1] for row in rows],
            "forward_return_5d": [row[2] for row in rows],
        }
    )


def test_validate_factor_calculates_complete_five_day_report() -> None:
    """固定两日五股票样本可以精确计算主要评价指标。"""
    trade_dates = [date(2024, 1, 2), date(2024, 1, 3)]
    factors = make_factors(trade_dates, [[1, 2, 3, 4, 5], [1, 2, 3, 4, 5]])
    labels = make_labels(trade_dates, [[0.01, 0.02, 0.03, 0.04, 0.05]] * 2)
    universe = factors.select("ts_code", "trade_date")
    metadata = build_default_registry().get("return_5d")

    report = validate_factor(factors, labels, universe, metadata)

    assert report.factor_name == "return_5d"
    assert report.factor_version == "v1"
    assert report.start_date == trade_dates[0]
    assert report.end_date == trade_dates[-1]
    assert report.row_count == 10
    assert report.trade_date_count == 2
    assert report.security_count == 5
    assert report.missing_value_rate == 0.0
    assert [item.coverage for item in report.coverage_by_date] == [1.0, 1.0]
    assert report.mean == pytest.approx(3.0)
    assert report.min == 1.0
    assert report.p25 == pytest.approx(2.0)
    assert report.median == pytest.approx(3.0)
    assert report.p75 == pytest.approx(4.0)
    assert report.max == 5.0
    assert report.ic_5d_mean == pytest.approx(1.0)
    assert report.rank_ic_5d_mean == pytest.approx(1.0)
    assert report.rank_ic_5d_std == pytest.approx(0.0)
    assert report.rank_ic_5d_count == 2
    assert report.ic_ir is None
    assert report.positive_ic_ratio == 1.0
    assert report.q1_return == pytest.approx(0.01)
    assert report.q2_return == pytest.approx(0.02)
    assert report.q3_return == pytest.approx(0.03)
    assert report.q4_return == pytest.approx(0.04)
    assert report.q5_return == pytest.approx(0.05)
    assert report.long_short_return == pytest.approx(-0.04)
    assert report.turnover == pytest.approx(0.0)
    assert report.factor_autocorr_1d == pytest.approx(1.0)


def test_validate_factor_reports_coverage_missing_values_and_insufficient_samples() -> None:
    """缺失和非有限因子计入缺失率,小截面不产生研究指标。"""
    trade_date = date(2024, 1, 2)
    factors = make_factors([trade_date], [[1.0, 2.0, None, float("inf")]])
    labels = make_labels([trade_date], [[0.01, 0.02, 0.03, 0.04]])
    universe = pl.concat(
        [
            factors.select("ts_code", "trade_date"),
            pl.DataFrame({"ts_code": ["999999.SZ"], "trade_date": [trade_date]}),
        ]
    )
    metadata = build_default_registry().get("return_5d")

    report = validate_factor(factors, labels, universe, metadata)

    assert report.missing_value_rate == pytest.approx(0.5)
    assert report.coverage_by_date[0].universe_count == 5
    assert report.coverage_by_date[0].valid_count == 2
    assert report.coverage_by_date[0].coverage == pytest.approx(0.4)
    assert report.rank_ic_5d_count == 0
    assert report.ic_5d_mean is None
    assert report.rank_ic_5d_mean is None
    assert report.q1_return is None
    assert report.long_short_return is None
    assert report.turnover is None
    assert report.factor_autocorr_1d is None


def test_validate_factor_uses_average_ranks_for_ties() -> None:
    """Spearman RankIC 对并列因子值使用平均排名。"""
    trade_date = date(2024, 1, 2)
    factors = make_factors([trade_date], [[1, 2, 2, 4, 5]])
    labels = make_labels([trade_date], [[0.01, 0.02, 0.03, 0.04, 0.05]])
    metadata = build_default_registry().get("return_5d")

    report = validate_factor(
        factors,
        labels,
        factors.select("ts_code", "trade_date"),
        metadata,
    )

    assert report.rank_ic_5d_count == 1
    assert report.rank_ic_5d_mean == pytest.approx(0.9746794344808963)
    assert report.rank_ic_5d_std is None


def test_validate_factor_uses_metadata_direction_for_turnover_and_long_short() -> None:
    """高值优先时使用 Q5 组合,并按相邻日期估算等权换手。"""
    trade_dates = [date(2024, 1, 2), date(2024, 1, 3)]
    factors = make_factors(trade_dates, [[1, 2, 3, 4, 5], [5, 2, 3, 4, 1]])
    labels = make_labels(trade_dates, [[0.01, 0.02, 0.03, 0.04, 0.05]] * 2)
    metadata = replace(build_default_registry().get("return_5d"), higher_is_better=True)

    report = validate_factor(
        factors,
        labels,
        factors.select("ts_code", "trade_date"),
        metadata,
    )

    assert report.turnover == pytest.approx(1.0)
    assert report.long_short_return == pytest.approx(0.0)
    assert report.factor_autocorr_1d == pytest.approx(-0.6)


def test_validate_factor_uses_universe_dates_as_report_range() -> None:
    """报告区间和覆盖率包含股票池中没有因子记录的边界日期。"""
    trade_dates = [date(2024, 1, 2), date(2024, 1, 3)]
    factors = make_factors([trade_dates[0]], [[1, 2, 3, 4, 5]])
    labels = make_labels([trade_dates[0]], [[0.01, 0.02, 0.03, 0.04, 0.05]])
    universe = make_labels(trade_dates, [[None] * 5, [None] * 5]).select("ts_code", "trade_date")

    report = validate_factor(
        factors,
        labels,
        universe,
        build_default_registry().get("return_5d"),
    )

    assert report.start_date == trade_dates[0]
    assert report.end_date == trade_dates[1]
    assert [item.coverage for item in report.coverage_by_date] == [1.0, 0.0]


def test_validate_factor_does_not_skip_empty_middle_date_for_autocorrelation() -> None:
    """整日无有效因子时不会把前后非相邻日期用于一日自相关。"""
    trade_dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    factors = make_factors(
        trade_dates,
        [[1, 2, 3, 4, 5], [None] * 5, [1, 2, 3, 4, 5]],
    )
    labels = make_labels(trade_dates, [[0.01, 0.02, 0.03, 0.04, 0.05]] * 3)

    report = validate_factor(
        factors,
        labels,
        factors.select("ts_code", "trade_date"),
        build_default_registry().get("return_5d"),
    )

    assert report.factor_autocorr_1d is None


def test_validate_factor_averages_daily_metrics_with_equal_date_weights() -> None:
    """截面规模不同时 IC 和分组收益仍按日期等权。"""
    trade_dates = [date(2024, 1, 2), date(2024, 1, 3)]
    factors = make_factors(
        trade_dates,
        [[1, 2, 3, 4, 5], list(range(1, 11))],
    )
    labels = make_labels(
        trade_dates,
        [
            [0.01, 0.02, 0.03, 0.04, 0.05],
            [0.10, 0.09, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.02, 0.01],
        ],
    )

    report = validate_factor(
        factors,
        labels,
        factors.select("ts_code", "trade_date"),
        build_default_registry().get("return_5d"),
    )

    assert report.ic_5d_mean == pytest.approx(0.0)
    assert report.rank_ic_5d_mean == pytest.approx(0.0)
    assert report.q1_return == pytest.approx((0.01 + 0.095) / 2)


@pytest.mark.parametrize(
    ("factors", "error_message"),
    [
        (pl.DataFrame(), "因子评价输入不能为空"),
        (
            make_factors([date(2024, 1, 2)], [[1, 2, 3, 4, 5]]).with_columns(
                pl.lit("wrong_name").alias("factor_name")
            ),
            "因子评价身份不匹配",
        ),
        (
            pl.concat(
                [
                    make_factors([date(2024, 1, 2)], [[1, 2, 3, 4, 5]]),
                    make_factors([date(2024, 1, 2)], [[1, 2, 3, 4, 5]]).head(1),
                ]
            ),
            "因子评价输入包含重复键",
        ),
    ],
)
def test_validate_factor_rejects_invalid_factor_input(
    factors: pl.DataFrame,
    error_message: str,
) -> None:
    """空输入、错误身份和重复键在评价前失败。"""
    trade_date = date(2024, 1, 2)
    labels = make_labels([trade_date], [[0.01, 0.02, 0.03, 0.04, 0.05]])
    universe = labels.select("ts_code", "trade_date")
    metadata = build_default_registry().get("return_5d")

    with pytest.raises(ValueError, match=error_message):
        validate_factor(factors, labels, universe, metadata)


def test_validate_factor_rejects_factor_date_outside_universe_or_labels() -> None:
    """因子日期必须同时存在于股票池和标签日期范围。"""
    factor_date = date(2024, 1, 2)
    other_date = date(2024, 1, 3)
    factors = make_factors([factor_date], [[1, 2, 3, 4, 5]])
    metadata = build_default_registry().get("return_5d")

    with pytest.raises(ValueError, match="因子日期不在股票池日期范围内"):
        validate_factor(
            factors,
            make_labels([factor_date], [[0.01, 0.02, 0.03, 0.04, 0.05]]),
            make_labels([other_date], [[None] * 5]).select("ts_code", "trade_date"),
            metadata,
        )
    with pytest.raises(ValueError, match="因子日期不在未来收益标签日期范围内"):
        validate_factor(
            factors,
            make_labels([other_date], [[0.01, 0.02, 0.03, 0.04, 0.05]]),
            factors.select("ts_code", "trade_date"),
            metadata,
        )


def test_run_factor_validation_reads_complete_report_without_writing(tmp_path: Path) -> None:
    """运行入口读取未来价格完成评价,且不改写因子或持久化标签。"""
    config = make_validation_config(tmp_path)
    trade_dates = [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
        date(2024, 1, 9),
        date(2024, 1, 10),
        date(2024, 1, 11),
    ]
    write_validation_market_data(config.paths.processed_dir, trade_dates)
    factor_frame = make_factors(trade_dates[:2], [[1, 2, 3, 4, 5]] * 2)
    written = write_factor_results(config.paths.processed_dir, factor_frame.to_pandas())
    factor_path = next(iter(written))
    original_factor_bytes = factor_path.read_bytes()
    initialize_validation_calendar(config, trade_dates)

    report = run_factor_validation(
        config,
        "return_5d",
        trade_dates[0],
        trade_dates[1],
    )

    assert report.row_count == 10
    assert report.rank_ic_5d_count == 2
    assert report.rank_ic_5d_mean == pytest.approx(1.0)
    assert report.long_short_return == pytest.approx(-0.04)
    assert [item.coverage for item in report.coverage_by_date] == [1.0, 1.0]
    assert factor_path.read_bytes() == original_factor_bytes
    assert list(config.paths.processed_dir.rglob("*label*")) == []


def test_run_factor_validation_validates_request_before_reading(tmp_path: Path) -> None:
    """反向日期和未知因子在访问数据前失败。"""
    config = make_validation_config(tmp_path)

    with pytest.raises(ValueError, match="start_date 不能晚于 end_date"):
        run_factor_validation(config, "return_5d", date(2024, 1, 3), date(2024, 1, 2))
    with pytest.raises(KeyError, match="未注册因子"):
        run_factor_validation(config, "unknown", date(2024, 1, 2), date(2024, 1, 3))


def make_validation_config(tmp_path: Path) -> QuantConfig:
    """创建隔离的 Validation 配置。"""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.toml").write_text(
        f"""
[project]
name = "test"

[paths]
raw_dir = "{(tmp_path / "raw").as_posix()}"
processed_dir = "{(tmp_path / "processed").as_posix()}"
database_path = "{(tmp_path / "db" / "quant.duckdb").as_posix()}"
notebooks_dir = "{(tmp_path / "notebooks").as_posix()}"
log_dir = "{(tmp_path / "log").as_posix()}"
""",
        encoding="utf-8",
    )
    return load_config(config_dir=config_dir)


def initialize_validation_calendar(config: QuantConfig, trade_dates: list[date]) -> None:
    """初始化 DuckDB 并写入 SSE 开市日。"""
    manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
    manager.initialize()
    with manager.session() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO dim_trade_calendar
                (exchange, cal_date, is_open, pretrade_date)
            VALUES ('SSE', ?, TRUE, NULL)
            """,
            [(trade_date,) for trade_date in trade_dates],
        )


def write_validation_market_data(processed_dir: Path, trade_dates: list[date]) -> None:
    """写入五只股票和八个交易日的最小 HFQ 行情。"""
    ts_codes = [f"{index:06d}.SZ" for index in range(1, 6)]
    rows = [(ts_code, trade_date) for ts_code in ts_codes for trade_date in trade_dates]
    opens = []
    for ts_code, trade_date in rows:
        security_index = ts_codes.index(ts_code) + 1
        if trade_date in (trade_dates[6], trade_dates[7]):
            opens.append(100.0 + security_index)
        else:
            opens.append(100.0)

    ohlcv_path = processed_dir / "ohlcv" / "year=2024" / "month=01" / "bars.parquet"
    ohlcv_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_code": [row[0] for row in rows],
            "trade_date": [row[1] for row in rows],
            "open": opens,
            "high": opens,
            "low": opens,
            "close": opens,
            "pre_close": opens,
            "change": [0.0] * len(rows),
            "pct_chg": [0.0] * len(rows),
            "volume": [1_000.0] * len(rows),
            "amount": [100_000.0] * len(rows),
            "is_suspended": [False] * len(rows),
            "is_st": [False] * len(rows),
            "limit_status": [0] * len(rows),
        }
    ).write_parquet(ohlcv_path)

    adj_path = processed_dir / "adj_factor" / "year=2024" / "month=01" / "adj.parquet"
    adj_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_code": [row[0] for row in rows],
            "trade_date": [row[1] for row in rows],
            "cumulative_factor": [1.0] * len(rows),
        }
    ).write_parquet(adj_path)
