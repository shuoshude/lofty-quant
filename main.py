from loguru import logger

from quant.config import QuantConfig, load_config
from quant.logger import setup_logger


def main() -> None:
    setup_logger()
    logger.info("lofty-quant started")

    config: QuantConfig = load_config()
    logger.info(config.paths.raw_dir)


if __name__ == "__main__":
    main()
