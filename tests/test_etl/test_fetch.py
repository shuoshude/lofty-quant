from datetime import date
from pathlib import Path

import pandas as pd

from quant.config import load_config
from quant.etl import ETLTask
from quant.etl.fetch import (
    fetch_raw_data,
    find_raw_files,
    read_raw_csv,
    write_raw_csv,
)
from quant.utils import build_raw_path


def test_raw_csv_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "raw.csv"
    df = pd.DataFrame([{"cal_date": "20240102", "is_open": 1}])

    row_count = write_raw_csv(path, df)

    assert row_count == 1
    expected = pd.DataFrame([{"cal_date": "20240102", "is_open": "1"}])
    pd.testing.assert_frame_equal(read_raw_csv(path), expected)


def test_trade_calendar_raw_path_uses_single_file(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
    )
    path = build_raw_path(tmp_path, task)

    assert path == tmp_path / "tushare" / "trade-calendar" / "trade-calendar_tushare.csv"


def test_stock_basic_raw_path_uses_single_file(tmp_path: Path) -> None:
    task = ETLTask(
        dataset="stock-basic",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
    )
    path = build_raw_path(tmp_path, task)

    assert path == tmp_path / "tushare" / "stock-basic" / "stock-basic_tushare.csv"


def test_raw_only_daily_dataset_paths_use_daily_file_layout(tmp_path: Path) -> None:
    for dataset in ("stock-st", "stk-limit", "suspend-d"):
        task = ETLTask(
            dataset=dataset,
            source="tushare",
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 31),
        )

        assert build_raw_path(tmp_path, task) == (
            tmp_path
            / "tushare"
            / dataset
            / "year=2024"
            / "month=01"
            / f"{dataset}_tushare_20240102.csv"
        )


def test_partitioned_raw_files_scan_multiple_months(tmp_path: Path) -> None:
    january_task = ETLTask(
        dataset="daily-ohlcv",
        source="akshare",
        start_date=date(2024, 1, 31),
        end_date=date(2024, 1, 31),
    )
    february_task = ETLTask(
        dataset="daily-ohlcv",
        source="akshare",
        start_date=date(2024, 2, 1),
        end_date=date(2024, 2, 1),
    )
    january_path = build_raw_path(tmp_path, january_task)
    february_path = build_raw_path(tmp_path, february_task)
    out_of_range_path = build_raw_path(
        tmp_path,
        ETLTask(
            dataset="daily-ohlcv",
            source="akshare",
            start_date=date(2024, 1, 30),
            end_date=date(2024, 1, 30),
        ),
    )
    legacy_range_path = (
        tmp_path
        / "akshare"
        / "daily-ohlcv"
        / "year=2024"
        / "month=01"
        / "daily-ohlcv_akshare_20240131_20240201.csv"
    )
    write_raw_csv(january_path, pd.DataFrame([{"trade_date": "20240131"}]))
    write_raw_csv(february_path, pd.DataFrame([{"trade_date": "20240201"}]))
    write_raw_csv(out_of_range_path, pd.DataFrame([{"trade_date": "20240130"}]))
    write_raw_csv(legacy_range_path, pd.DataFrame([{"trade_date": "20240131"}]))

    task = ETLTask(
        dataset="daily-ohlcv",
        source="akshare",
        start_date=date(2024, 1, 31),
        end_date=date(2024, 2, 1),
    )

    assert find_raw_files(tmp_path, task) == [january_path, february_path]


def test_fetch_raw_data_uses_config_raw_dir(monkeypatch, tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
    )

    def fake_fetch_tushare_raw(_config, _task):
        return pd.DataFrame([{"cal_date": "20240102", "is_open": 1}])

    monkeypatch.setattr("quant.etl.fetch._fetch_tushare_raw", fake_fetch_tushare_raw)

    paths = fetch_raw_data(config, task)

    expected_path = (
        config.paths.raw_dir / "tushare" / "trade-calendar" / "trade-calendar_tushare.csv"
    )
    assert paths == (expected_path,)
    assert expected_path.exists()


def test_fetch_raw_data_skips_existing_raw_without_force(monkeypatch, tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
    )
    expected_path = build_raw_path(config.paths.raw_dir, task)
    write_raw_csv(expected_path, pd.DataFrame([{"cal_date": "20240101", "is_open": 0}]))

    def fake_fetch_tushare_raw(_config, _task):
        return pd.DataFrame([{"cal_date": "20240102", "is_open": 1}])

    monkeypatch.setattr("quant.etl.fetch._fetch_tushare_raw", fake_fetch_tushare_raw)

    paths = fetch_raw_data(config, task)

    assert paths == (expected_path,)
    assert read_raw_csv(expected_path)["cal_date"].tolist() == ["20240101"]


def test_fetch_raw_data_overwrites_existing_raw_with_force(monkeypatch, tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    task = ETLTask(
        dataset="trade-calendar",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        exchange="SSE",
        force=True,
    )
    expected_path = build_raw_path(config.paths.raw_dir, task)
    write_raw_csv(expected_path, pd.DataFrame([{"cal_date": "20240101", "is_open": 0}]))

    def fake_fetch_tushare_raw(_config, _task):
        return pd.DataFrame([{"cal_date": "20240102", "is_open": 1}])

    monkeypatch.setattr("quant.etl.fetch._fetch_tushare_raw", fake_fetch_tushare_raw)

    paths = fetch_raw_data(config, task)

    assert paths == (expected_path,)
    assert read_raw_csv(expected_path)["cal_date"].tolist() == ["20240102"]


def test_fetch_raw_data_writes_daily_ohlcv_daily_files(monkeypatch, tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    task = ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        exchange="SSE",
    )

    expected_paths = (
        config.paths.raw_dir
        / "tushare"
        / "daily-ohlcv"
        / "year=2024"
        / "month=01"
        / "daily-ohlcv_tushare_20240102.csv",
        config.paths.raw_dir
        / "tushare"
        / "daily-ohlcv"
        / "year=2024"
        / "month=01"
        / "daily-ohlcv_tushare_20240103.csv",
    )

    def fake_fetch_tushare_raw(_config, _task):
        yield date(2024, 1, 2), pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240102",
                    "open": 10.0,
                    "close": 10.2,
                }
            ]
        )
        assert expected_paths[0].exists()
        yield date(2024, 1, 3), pd.DataFrame(
            [
                {
                    "ts_code": "000002.SZ",
                    "trade_date": "20240103",
                    "open": 20.0,
                    "close": 20.2,
                }
            ]
        )

    monkeypatch.setattr("quant.etl.fetch._fetch_tushare_raw", fake_fetch_tushare_raw)

    paths = fetch_raw_data(config, task)

    assert paths == expected_paths
    expected = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240102",
                "open": "10.0",
                "close": "10.2",
            }
        ]
    )
    pd.testing.assert_frame_equal(read_raw_csv(paths[0]), expected)
    assert read_raw_csv(paths[1])["trade_date"].tolist() == ["20240103"]


def test_fetch_raw_data_dry_run_does_not_write_daily_ohlcv(monkeypatch, tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    task = ETLTask(
        dataset="daily-ohlcv",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        dry_run=True,
    )

    def fake_fetch_tushare_raw(_config, _task):
        yield date(2024, 1, 2), pd.DataFrame(
            [{"ts_code": "000001.SZ", "trade_date": "20240102"}],
        )

    monkeypatch.setattr("quant.etl.fetch._fetch_tushare_raw", fake_fetch_tushare_raw)

    paths = fetch_raw_data(config, task)

    expected_path = (
        config.paths.raw_dir
        / "tushare"
        / "daily-ohlcv"
        / "year=2024"
        / "month=01"
        / "daily-ohlcv_tushare_20240102.csv"
    )
    assert paths == (expected_path,)
    assert not expected_path.exists()


def test_fetch_raw_data_writes_adj_factor_daily_files(monkeypatch, tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    task = ETLTask(
        dataset="adj-factor",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        exchange="SSE",
    )

    expected_paths = (
        config.paths.raw_dir
        / "tushare"
        / "adj-factor"
        / "year=2024"
        / "month=01"
        / "adj-factor_tushare_20240102.csv",
        config.paths.raw_dir
        / "tushare"
        / "adj-factor"
        / "year=2024"
        / "month=01"
        / "adj-factor_tushare_20240103.csv",
    )

    def fake_fetch_tushare_raw(_config, _task):
        yield date(2024, 1, 2), pd.DataFrame(
            [{"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 2.0}],
        )
        assert expected_paths[0].exists()
        yield date(2024, 1, 3), pd.DataFrame(
            [{"ts_code": "000001.SZ", "trade_date": "20240103", "adj_factor": 2.1}],
        )

    monkeypatch.setattr("quant.etl.fetch._fetch_tushare_raw", fake_fetch_tushare_raw)

    paths = fetch_raw_data(config, task)

    assert paths == expected_paths
    assert read_raw_csv(paths[0]).to_dict(orient="records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": "2.0"}
    ]


def test_fetch_raw_data_writes_daily_basic_daily_files(monkeypatch, tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    task = ETLTask(
        dataset="daily-basic",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        exchange="SSE",
    )

    expected_paths = (
        config.paths.raw_dir
        / "tushare"
        / "daily-basic"
        / "year=2024"
        / "month=01"
        / "daily-basic_tushare_20240102.csv",
        config.paths.raw_dir
        / "tushare"
        / "daily-basic"
        / "year=2024"
        / "month=01"
        / "daily-basic_tushare_20240103.csv",
    )

    def fake_fetch_tushare_raw(_config, _task):
        yield date(2024, 1, 2), pd.DataFrame(
            [{"ts_code": "000001.SZ", "trade_date": "20240102", "turnover_rate": 1.5}],
        )
        assert expected_paths[0].exists()
        yield date(2024, 1, 3), pd.DataFrame(
            [{"ts_code": "000002.SZ", "trade_date": "20240103", "turnover_rate": 2.5}],
        )

    monkeypatch.setattr("quant.etl.fetch._fetch_tushare_raw", fake_fetch_tushare_raw)

    paths = fetch_raw_data(config, task)

    assert paths == expected_paths
    assert read_raw_csv(paths[0]).to_dict(orient="records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "turnover_rate": "1.5"}
    ]


def test_fetch_raw_data_writes_stock_basic_single_file(monkeypatch, tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    task = ETLTask(
        dataset="stock-basic",
        source="tushare",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
    )

    def fake_fetch_tushare_raw(_config, _task):
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "list_status": "L",
                }
            ]
        )

    monkeypatch.setattr("quant.etl.fetch._fetch_tushare_raw", fake_fetch_tushare_raw)

    paths = fetch_raw_data(config, task)

    expected_path = config.paths.raw_dir / "tushare" / "stock-basic" / "stock-basic_tushare.csv"
    assert paths == (expected_path,)
    assert read_raw_csv(expected_path)["ts_code"].tolist() == ["000001.SZ"]


def test_fetch_raw_data_writes_raw_only_daily_files(monkeypatch, tmp_path: Path) -> None:
    config = load_config(config_dir=make_config_dir(tmp_path))
    task = ETLTask(
        dataset="stock-st",
        source="tushare",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        exchange="SSE",
    )

    expected_paths = (
        config.paths.raw_dir
        / "tushare"
        / "stock-st"
        / "year=2024"
        / "month=01"
        / "stock-st_tushare_20240102.csv",
        config.paths.raw_dir
        / "tushare"
        / "stock-st"
        / "year=2024"
        / "month=01"
        / "stock-st_tushare_20240103.csv",
    )

    def fake_fetch_tushare_raw(_config, _task):
        yield date(2024, 1, 2), pd.DataFrame(
            [{"ts_code": "000001.SZ", "trade_date": "20240102", "type": "ST"}],
        )
        assert expected_paths[0].exists()
        yield date(2024, 1, 3), pd.DataFrame(
            [{"ts_code": "000002.SZ", "trade_date": "20240103", "type": "*ST"}],
        )

    monkeypatch.setattr("quant.etl.fetch._fetch_tushare_raw", fake_fetch_tushare_raw)

    paths = fetch_raw_data(config, task)

    assert paths == expected_paths
    assert read_raw_csv(paths[0]).to_dict(orient="records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "type": "ST"}
    ]


def make_config_dir(tmp_path: Path) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
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
    return config_dir
