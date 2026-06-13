from datetime import date

from loguru import logger

from quant.config import load_config
from quant.etl import ETLTask, fetch_raw_data
from quant.logger import setup_logger


def main() -> None:
    setup_logger()
    config = load_config()
    logger.info("测试")
    task = ETLTask(
        source="tushare",
        dataset="trade-calendar",
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 13),
        exchange="SSE",
    )
    fetch_raw_data(config, task)


if __name__ == "__main__":
    main()
