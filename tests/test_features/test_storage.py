from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from quant.data.db import DuckDBManager
from quant.data.repository import QuantRepository
from quant.features import write_factor_results


def test_write_factor_results_writes_single_month_with_fixed_schema(tmp_path: Path) -> None:
    """单月因子结果按固定九列写入标准路径。"""
    factor_df = make_factor_df(
        [
            ("000001.SZ", date(2024, 1, 2), "momentum_20d", 0.12, "v1"),
        ]
    )

    written = write_factor_results(tmp_path, factor_df)

    output_path = tmp_path / "factors" / "year=2024" / "month=01" / "factors_202401.parquet"
    output_df = pd.read_parquet(output_path)
    assert written == {output_path: 1}
    assert output_df.columns.tolist() == [
        "ts_code",
        "trade_date",
        "factor_name",
        "factor_value",
        "factor_version",
        "raw_value",
        "processed_value",
        "quality_status",
        "created_at",
    ]
    assert output_df.loc[0, "factor_value"] == 0.12
    assert (
        output_df.loc[
            0,
            ["raw_value", "processed_value", "quality_status", "created_at"],
        ]
        .isna()
        .all()
    )


def test_write_factor_results_splits_cross_month_data(tmp_path: Path) -> None:
    """跨月因子结果拆分到各自月份的标准文件。"""
    factor_df = make_factor_df(
        [
            ("000001.SZ", date(2024, 1, 31), "momentum_20d", 0.12, "v1"),
            ("000002.SZ", date(2024, 2, 1), "momentum_20d", 0.08, "v1"),
        ]
    )

    written = write_factor_results(tmp_path, factor_df)

    assert set(written) == {
        tmp_path / "factors" / "year=2024" / "month=01" / "factors_202401.parquet",
        tmp_path / "factors" / "year=2024" / "month=02" / "factors_202402.parquet",
    }


def test_write_factor_results_overwrites_only_matching_unique_key(tmp_path: Path) -> None:
    """后写记录只覆盖名称和版本都相同的因子键。"""
    initial_df = make_factor_df(
        [
            ("000001.SZ", date(2024, 1, 2), "momentum_20d", 0.10, "v1"),
            ("000001.SZ", date(2024, 1, 2), "momentum_20d", 0.20, "v2"),
            ("000001.SZ", date(2024, 1, 2), "volatility_20d", 0.30, "v1"),
        ]
    )
    replacement_df = make_factor_df([("000001.SZ", date(2024, 1, 2), "momentum_20d", 0.90, "v1")])

    write_factor_results(tmp_path, initial_df)
    write_factor_results(tmp_path, replacement_df)

    output_path = tmp_path / "factors" / "year=2024" / "month=01" / "factors_202401.parquet"
    output_df = pd.read_parquet(output_path)
    values = {
        (row.factor_name, row.factor_version): row.factor_value
        for row in output_df.itertuples(index=False)
    }
    assert values == {
        ("momentum_20d", "v1"): 0.90,
        ("momentum_20d", "v2"): 0.20,
        ("volatility_20d", "v1"): 0.30,
    }


def test_write_factor_results_skips_empty_dataframe(tmp_path: Path) -> None:
    """空结果不要求 schema 且不创建存储目录。"""
    written = write_factor_results(tmp_path, pd.DataFrame())

    assert written == {}
    assert not (tmp_path / "factors").exists()


def test_write_factor_results_rejects_missing_required_columns(tmp_path: Path) -> None:
    """非空因子结果缺少兼容字段时返回清晰错误。"""
    factor_df = make_factor_df([("000001.SZ", date(2024, 1, 2), "momentum_20d", 0.12, "v1")]).drop(
        columns="factor_value"
    )

    with pytest.raises(ValueError, match=r"因子结果缺少字段: \['factor_value'\]"):
        write_factor_results(tmp_path, factor_df)


def test_write_factor_results_rejects_unknown_columns(tmp_path: Path) -> None:
    """未知额外字段不能在写入时被静默丢弃。"""
    factor_df = make_factor_df(
        [("000001.SZ", date(2024, 1, 2), "momentum_20d", 0.12, "v1")]
    ).assign(signal=1)

    with pytest.raises(ValueError, match=r"因子结果包含不支持的字段: \['signal'\]"):
        write_factor_results(tmp_path, factor_df)


def test_write_factor_results_preserves_optional_research_columns(tmp_path: Path) -> None:
    """标准研究字段写入后保持原值。"""
    created_at = datetime(2024, 1, 2, 18, 30)
    factor_df = make_factor_df(
        [("000001.SZ", date(2024, 1, 2), "momentum_20d", 0.80, "v1")]
    ).assign(
        raw_value=0.12,
        processed_value=0.80,
        quality_status="valid",
        created_at=created_at,
    )

    write_factor_results(tmp_path, factor_df)

    output_path = tmp_path / "factors" / "year=2024" / "month=01" / "factors_202401.parquet"
    output_df = pd.read_parquet(output_path)
    assert output_df.loc[0, "raw_value"] == 0.12
    assert output_df.loc[0, "processed_value"] == 0.80
    assert output_df.loc[0, "quality_status"] == "valid"
    assert output_df.loc[0, "created_at"] == created_at


def test_write_factor_results_keeps_optional_columns_during_partial_overwrite(
    tmp_path: Path,
) -> None:
    """缺少可选字段的覆盖不会删除同月其他记录的研究值。"""
    created_at = datetime(2024, 1, 2, 18, 30)
    initial_df = make_factor_df(
        [
            ("000001.SZ", date(2024, 1, 2), "momentum_20d", 0.80, "v1"),
            ("000002.SZ", date(2024, 1, 2), "momentum_20d", 0.60, "v1"),
        ]
    ).assign(
        raw_value=[0.12, 0.08],
        processed_value=[0.80, 0.60],
        quality_status=["valid", "valid"],
        created_at=[created_at, created_at],
    )
    replacement_df = make_factor_df([("000001.SZ", date(2024, 1, 2), "momentum_20d", 0.90, "v1")])

    write_factor_results(tmp_path, initial_df)
    write_factor_results(tmp_path, replacement_df)

    output_path = tmp_path / "factors" / "year=2024" / "month=01" / "factors_202401.parquet"
    output_df = pd.read_parquet(output_path).set_index("ts_code")
    assert pd.isna(output_df.loc["000001.SZ", "raw_value"])
    assert pd.isna(output_df.loc["000001.SZ", "quality_status"])
    assert output_df.loc["000002.SZ", "raw_value"] == 0.08
    assert output_df.loc["000002.SZ", "quality_status"] == "valid"
    assert output_df.loc["000002.SZ", "created_at"] == created_at


def test_factor_results_remain_compatible_with_duckdb_repository(tmp_path: Path) -> None:
    """固定九列可以注册为视图且不改变 Repository 查询契约。"""
    factor_df = make_factor_df(
        [("000001.SZ", date(2024, 1, 2), "momentum_20d", 0.80, "v1")]
    ).assign(
        raw_value=0.12,
        processed_value=0.80,
        quality_status="valid",
        created_at=datetime(2024, 1, 2, 18, 30),
    )
    write_factor_results(tmp_path, factor_df)
    manager = DuckDBManager(tmp_path / "quant.duckdb", tmp_path)
    manager.initialize()

    with manager.session() as conn:
        rows = QuantRepository(conn).get_factors(
            date(2024, 1, 2),
            ["momentum_20d"],
            factor_version="v1",
        )

    assert rows == [
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2024, 1, 2),
            "factor_name": "momentum_20d",
            "factor_value": 0.80,
            "factor_version": "v1",
        }
    ]


def make_factor_df(
    rows: list[tuple[str, date, str, float, str]],
) -> pd.DataFrame:
    """构造只包含兼容字段的因子测试数据。"""
    return pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "trade_date": trade_date,
                "factor_name": factor_name,
                "factor_value": factor_value,
                "factor_version": factor_version,
            }
            for ts_code, trade_date, factor_name, factor_value, factor_version in rows
        ]
    )
