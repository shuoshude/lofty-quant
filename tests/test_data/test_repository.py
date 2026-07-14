from datetime import date
from pathlib import Path

import polars as pl
import pytest

from quant.data.db import DuckDBManager
from quant.data.repository import QuantRepository


def test_repository_returns_daily_bars_ordered_by_date(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        rows = repository.get_daily_bars(
            "000001.SZ",
            date(2024, 1, 1),
            date(2024, 1, 3),
            adjustment="qfq",
            as_of_date=date(2024, 1, 3),
        )

    assert [row["trade_date"] for row in rows] == [date(2024, 1, 2), date(2024, 1, 3)]
    assert [row["qfq_close"] for row in rows] == pytest.approx([7.875, 12.0])


def test_repository_returns_hfq_daily_bars(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        rows = repository.get_daily_bars(
            "000001.SZ",
            date(2024, 1, 1),
            date(2024, 1, 3),
            adjustment="hfq",
        )

    assert [row["hfq_close"] for row in rows] == pytest.approx([15.75, 24.0])


def test_repository_returns_hfq_daily_panel_as_polars_dataframe(tmp_path: Path) -> None:
    """全市场 HFQ 面板自动包含键并按证券和日期排序。"""
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        panel = repository.get_daily_panel(
            date(2024, 1, 2),
            date(2024, 1, 3),
            ["hfq_close", "amount"],
            adjustment="hfq",
        )

    assert isinstance(panel, pl.DataFrame)
    assert panel.columns == ["ts_code", "trade_date", "hfq_close", "amount"]
    assert panel.to_dicts() == [
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2024, 1, 2),
            "hfq_close": 15.75,
            "amount": 10500.0,
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2024, 1, 3),
            "hfq_close": 24.0,
            "amount": 18000.0,
        },
        {
            "ts_code": "000002.SZ",
            "trade_date": date(2024, 1, 2),
            "hfq_close": 5.1,
            "amount": 10200.0,
        },
    ]


def test_repository_daily_panel_supports_unadjusted_fields_without_duplicate_keys(
    tmp_path: Path,
) -> None:
    """未复权面板返回原始字段且自动去除重复键列。"""
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        panel = repository.get_daily_panel(
            date(2024, 1, 2),
            date(2024, 1, 2),
            ["trade_date", "close", "ts_code"],
            adjustment="none",
        )

    assert panel.columns == ["ts_code", "trade_date", "close"]
    assert panel["close"].to_list() == [10.5, 5.1]


def test_repository_daily_panel_rejects_fields_unavailable_for_adjustment(
    tmp_path: Path,
) -> None:
    """研究面板在查询前拒绝当前复权视图不存在的字段。"""
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        with pytest.raises(
            ValueError,
            match=r"研究面板字段不适用于 adjustment=none: \['hfq_close'\]",
        ):
            repository.get_daily_panel(
                date(2024, 1, 2),
                date(2024, 1, 3),
                ["hfq_close"],
                adjustment="none",
            )


@pytest.mark.parametrize(
    ("fields", "error_message"),
    [
        ([], "fields 不能为空"),
        (["close; DROP TABLE v_daily_ohlcv"], "无效的字段名"),
    ],
)
def test_repository_daily_panel_validates_requested_field_names(
    tmp_path: Path,
    fields: list[str],
    error_message: str,
) -> None:
    """研究面板复用 Repository 的字段名安全校验。"""
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        with pytest.raises(ValueError, match=error_message):
            repository.get_daily_panel(
                date(2024, 1, 2),
                date(2024, 1, 3),
                fields,
            )


def test_repository_daily_panel_rejects_qfq_adjustment(tmp_path: Path) -> None:
    """研究面板拒绝可能引入历史口径偏差的 QFQ 模式。"""
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        with pytest.raises(ValueError, match="研究面板不支持的复权模式: qfq"):
            repository.get_daily_panel(
                date(2024, 1, 2),
                date(2024, 1, 3),
                ["close"],
                adjustment="qfq",  # type: ignore[arg-type]
            )


def test_repository_qfq_as_of_date_does_not_use_future_factor(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        rows = repository.get_daily_bars(
            "000001.SZ",
            date(2024, 1, 1),
            date(2024, 1, 3),
            adjustment="qfq",
            as_of_date=date(2024, 1, 2),
        )

    assert [row["trade_date"] for row in rows] == [date(2024, 1, 2)]
    assert [row["qfq_close"] for row in rows] == pytest.approx([10.5])


def test_repository_cross_section_can_exclude_suspended_rows(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        rows = repository.get_cross_section(
            date(2024, 1, 2),
            ["ts_code", "close", "is_suspended"],
            exclude_suspended=True,
        )

    assert rows == [{"ts_code": "000001.SZ", "close": 10.5, "is_suspended": False}]


def test_repository_filters_factors_by_name_and_version(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        repository = QuantRepository(conn)
        rows = repository.get_factors(
            date(2024, 1, 2),
            ["momentum_20d", "size"],
            factor_version="v1",
        )

    assert rows == [
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2024, 1, 2),
            "factor_name": "momentum_20d",
            "factor_value": 1.23,
            "factor_version": "v1",
        }
    ]


def test_repository_returns_factor_panel_filtered_and_ordered(tmp_path: Path) -> None:
    """因子区间面板按名称、版本、证券和日期稳定过滤。"""
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        panel = QuantRepository(conn).get_factor_panel(
            date(2024, 1, 2),
            date(2024, 1, 3),
            "momentum_20d",
            factor_version="v1",
        )

    assert isinstance(panel, pl.DataFrame)
    assert panel.columns == [
        "ts_code",
        "trade_date",
        "factor_name",
        "factor_value",
        "factor_version",
    ]
    assert panel.to_dicts() == [
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2024, 1, 2),
            "factor_name": "momentum_20d",
            "factor_value": 1.23,
            "factor_version": "v1",
        },
        {
            "ts_code": "000002.SZ",
            "trade_date": date(2024, 1, 3),
            "factor_name": "momentum_20d",
            "factor_value": 2.34,
            "factor_version": "v1",
        },
    ]


def test_repository_returns_trade_calendar(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        conn.execute(
            """
            INSERT INTO dim_trade_calendar (exchange, cal_date, is_open, pretrade_date)
            VALUES
                ('SSE', DATE '2024-01-01', FALSE, DATE '2023-12-29'),
                ('SSE', DATE '2024-01-02', TRUE, DATE '2023-12-29')
            """
        )
        repository = QuantRepository(conn)
        rows = repository.get_trade_calendar(date(2024, 1, 1), date(2024, 1, 2))

    assert [row["is_open"] for row in rows] == [False, True]


def test_repository_returns_open_trade_dates_filtered_and_ordered(tmp_path: Path) -> None:
    manager = initialized_manager(tmp_path)

    with manager.session() as conn:
        conn.execute(
            """
            INSERT INTO dim_trade_calendar (exchange, cal_date, is_open, pretrade_date)
            VALUES
                ('SSE', DATE '2024-01-04', TRUE, DATE '2024-01-02'),
                ('SSE', DATE '2024-01-02', TRUE, DATE '2023-12-29'),
                ('SSE', DATE '2024-01-03', FALSE, DATE '2024-01-02'),
                ('SZSE', DATE '2024-01-02', TRUE, DATE '2023-12-29'),
                ('SSE', DATE '2024-01-05', TRUE, DATE '2024-01-04')
            """
        )
        repository = QuantRepository(conn)
        trade_dates = repository.get_open_trade_dates(
            date(2024, 1, 2),
            date(2024, 1, 4),
            exchange="SSE",
        )

    assert trade_dates == [date(2024, 1, 2), date(2024, 1, 4)]


def initialized_manager(tmp_path: Path) -> DuckDBManager:
    processed_dir = tmp_path / "processed"
    write_parquet(
        processed_dir / "ohlcv" / "year=2024" / "month=01" / "bars.parquet",
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000002.SZ"],
            "trade_date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 2)],
            "open": [10.0, 11.0, 5.0],
            "high": [11.0, 12.0, 5.5],
            "low": [9.5, 10.5, 4.8],
            "close": [10.5, 12.0, 5.1],
            "pre_close": [10.0, 10.5, 5.0],
            "change": [0.5, 1.5, 0.1],
            "pct_chg": [5.0, 14.29, 2.0],
            "volume": [1000.0, 1500.0, 2000.0],
            "amount": [10500.0, 18000.0, 10200.0],
            "is_suspended": [False, False, True],
            "is_st": [False, False, False],
            "limit_status": [0, 1, -1],
        },
    )
    write_parquet(
        processed_dir / "adj_factor" / "year=2024" / "month=01" / "adj.parquet",
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000002.SZ"],
            "trade_date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 2)],
            "cumulative_factor": [1.5, 2.0, 1.0],
        },
    )
    write_parquet(
        processed_dir / "factors" / "year=2024" / "month=01" / "factors.parquet",
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000002.SZ", "000001.SZ"],
            "trade_date": [
                date(2024, 1, 2),
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 3),
            ],
            "factor_name": ["momentum_20d", "momentum_20d", "momentum_20d", "size"],
            "factor_value": [1.23, 9.99, 2.34, 0.56],
            "factor_version": ["v1", "v2", "v1", "v1"],
        },
    )
    manager = DuckDBManager(tmp_path / "quant.duckdb", processed_dir)
    manager.initialize()
    return manager


def write_parquet(path: Path, data: dict[str, list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(data).write_parquet(path)
