from loguru import logger

from quant.config import load_config
from quant.data.db import DuckDBManager
from quant.logger import setup_logger


def main() -> None:
    setup_logger()
    config = load_config()
    manager = DuckDBManager(
        db_path=config.paths.database_path,
        processed_dir=config.paths.processed_dir,
    )
    manager.initialize()
    logger.info("测试db")


if __name__ == "__main__":
    main()
