from pathlib import Path

from loguru import logger

from quant.config import load_config
from quant.logger import LOG_FORMAT, setup_logger


def test_setup_logger_writes_to_configured_log_directory(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    write_settings(config_dir / "settings.toml", tmp_path / "runtime-logs")
    config = load_config(config_dir=config_dir)

    setup_logger(config=config, enable_console=False)
    logger.info("hello logging")
    logger.complete()

    log_files = list((tmp_path / "runtime-logs").glob("lofty-quant_*.log"))

    assert len(log_files) == 1
    content = log_files[0].read_text(encoding="utf-8")
    assert "INFO" in content
    assert "test_setup_logger_writes_to_configured_log_directory" in content
    assert "hello logging" in content


def test_log_format_includes_timestamp_level_location_and_message() -> None:
    assert "{time:YYYY-MM-DD HH:mm:ss.SSS}" in LOG_FORMAT
    assert "{level:<8}" in LOG_FORMAT
    assert "{name}:{function}:{line}" in LOG_FORMAT
    assert "{message}" in LOG_FORMAT


def write_settings(path: Path, log_dir: Path) -> None:
    path.write_text(
        f"""
[project]
name = "test"

[paths]
raw_dir = "data/raw"
processed_dir = "data/processed"
database_path = "data/db/test.duckdb"
notebooks_dir = "notebooks"
log_dir = "{log_dir}"
""",
        encoding="utf-8",
    )
