from loguru import logger

from quant.config import load_config
from quant.data.db import DuckDBManager
from quant.logger import setup_logger


def main() -> None:
    setup_logger()
    config = load_config()
    logger.info("测试")
    # 如需临时测试 fetch, 同时恢复:
    # from datetime import date
    # from quant.etl import ETLTask, fetch_raw_data
    # task = ETLTask(
    #     source="tushare",
    #     dataset="trade-calendar",
    #     start_date=date(2026, 6, 1),
    #     end_date=date(2026, 6, 13),
    #     exchange="SSE",
    # )
    # fetch_raw_data(config, task)
    manager = DuckDBManager(
        db_path=config.paths.database_path,
        processed_dir=config.paths.processed_dir,
    )
    manager.initialize()


if __name__ == "__main__":
    main()
