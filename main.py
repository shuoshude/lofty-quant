from loguru import logger

from quant.config import load_config
from quant.logger import setup_logger
from quant.utils import get_project_root, resolve_log_dir


def main() -> None:
    setup_logger()
    etl_logger = logger.bind(module="etl")
    logger.info("lofty-quant 启动")
    root = get_project_root()
    conf = load_config()
    log_dir = resolve_log_dir(None, conf)
    etl_logger.info("ETL 日志测试")
    print(conf)
    print(root)
    print(log_dir)
    logger.warning("结束了")


if __name__ == "__main__":
    main()
