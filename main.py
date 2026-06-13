from loguru import logger

from quant.logger import setup_logger
from quant.etl import fetch_raw_data, ETLTask
from quant.config import load_config
from datetime import date
from pathlib import Path

def main() -> None:
    setup_logger()
    config = load_config()
    logger.info("测试")
    task = ETLTask(source='tushare', dataset='trade-calendar', start_date=date(2026,6,1), end_date=date(2026,6,13))
    fetch_raw_data(config.paths.raw_dir, task)


if __name__ == "__main__":
    main()
