from datetime import date
from pathlib import Path

from quant.config import load_config
from quant.etl import ETLTask
from quant.utils import (
    build_raw_path,
    format_duckdb_path,
    get_project_root,
    iter_raw_partition_dirs,
    parse_daily_raw_file_date,
    resolve_log_dir,
    resolve_path,
)


def test_get_project_root_finds_repository_root() -> None:
    project_root = get_project_root()

    assert project_root.name == "lofty-quant"
    assert (project_root / "pyproject.toml").exists()


def test_resolve_log_dir_resolves_relative_path_from_project_root(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    write_settings(config_dir / "settings.toml")
    config = load_config(config_dir=config_dir)

    assert resolve_log_dir("custom-log", config) == get_project_root() / "custom-log"


def test_resolve_log_dir_keeps_absolute_path(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    write_settings(config_dir / "settings.toml")
    config = load_config(config_dir=config_dir)

    assert resolve_log_dir(tmp_path / "absolute-log", config) == tmp_path / "absolute-log"


def test_resolve_path_resolves_relative_path_from_base_dir(tmp_path: Path) -> None:
    assert resolve_path("relative/path", tmp_path) == tmp_path / "relative" / "path"


def test_resolve_path_keeps_absolute_path(tmp_path: Path) -> None:
    absolute_path = tmp_path / "absolute" / "path"

    assert resolve_path(absolute_path, get_project_root()) == absolute_path


def test_resolve_path_expands_home_dir() -> None:
    assert (
        resolve_path("~/lofty-quant-test", get_project_root())
        == (Path.home() / "lofty-quant-test").resolve()
    )


def test_format_duckdb_path_escapes_single_quote(tmp_path: Path) -> None:
    path = tmp_path / "quote's" / "data.parquet"

    assert format_duckdb_path(path) == path.as_posix().replace("'", "''")


def test_build_raw_path_uses_single_file_layout_for_trade_calendar(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
    )

    assert build_raw_path(tmp_path, task) == (
        tmp_path / "tushare" / "trade-calendar" / "trade-calendar_tushare.csv"
    )


def test_build_raw_path_uses_month_partition_for_large_dataset(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    assert build_raw_path(tmp_path, task) == (
        tmp_path
        / "tushare"
        / "daily-ohlcv"
        / "year=2024"
        / "month=01"
        / "daily-ohlcv_tushare_20240101.csv"
    )


def test_build_raw_path_uses_daily_file_layout_for_adj_factor(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="adj-factor",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 31),
    )

    assert build_raw_path(tmp_path, task) == (
        tmp_path
        / "tushare"
        / "adj-factor"
        / "year=2024"
        / "month=01"
        / "adj-factor_tushare_20240102.csv"
    )


def test_iter_raw_partition_dirs_spans_month_range(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 31),
        end_date=date(2024, 2, 1),
    )

    assert list(iter_raw_partition_dirs(tmp_path, task)) == [
        tmp_path / "tushare" / "daily-ohlcv" / "year=2024" / "month=01",
        tmp_path / "tushare" / "daily-ohlcv" / "year=2024" / "month=02",
    ]


def test_parse_daily_raw_file_date_returns_daily_date(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )
    path = tmp_path / "daily-ohlcv_tushare_20240102.csv"

    assert parse_daily_raw_file_date(path, task) == date(2024, 1, 2)


def test_parse_daily_raw_file_date_rejects_non_daily_names(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    assert parse_daily_raw_file_date(
        tmp_path / "daily-ohlcv_tushare_20240102_20240103.csv",
        task,
    ) is None
    assert parse_daily_raw_file_date(
        tmp_path / "daily-ohlcv_tushare_20240230.csv",
        task,
    ) is None
    assert parse_daily_raw_file_date(
        tmp_path / "daily-ohlcv_akshare_20240102.csv",
        task,
    ) is None
    assert parse_daily_raw_file_date(
        tmp_path / "trade-calendar_tushare_20240102.csv",
        task,
    ) is None


def write_settings(path: Path) -> None:
    path.write_text(
        """
[project]
name = "test"

[paths]
raw_dir = "data/raw"
processed_dir = "data/processed"
database_path = "data/db/test.duckdb"
notebooks_dir = "notebooks"
log_dir = "log"
""",
        encoding="utf-8",
    )
