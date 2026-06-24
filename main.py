import csv
from collections.abc import Iterator
from pathlib import Path

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
    # )
    # fetch_raw_data(config, task)
    manager = DuckDBManager(
        db_path=config.paths.database_path,
        processed_dir=config.paths.processed_dir,
    )
    manager.initialize()
    # write_csv_in_batches(
    #     file_path="./users.csv",
    #     total=10000,
    #     batch_size=1000,
    # )


def generate_rows(total: int, batch_size: int) -> Iterator[list[tuple[int, str]]]:
    """
    分批生成数据。

    每次 yield 一个批次, 而不是一次性返回全部数据。
    """
    batch: list[tuple[int, str]] = []

    for user_id in range(1, total + 1):
        batch.append((user_id, f"user_{user_id}"))

        if len(batch) >= batch_size:
            yield batch
            batch = []

    # 处理最后一个不足 batch_size 的批次
    if batch:
        yield batch

def write_csv_in_batches(
    file_path: str | Path,
    total: int,
    batch_size: int = 1000,
) -> None:
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with file_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # 写表头
        writer.writerow(["user_id", "username"])

        # 每次只获取并写入一个批次
        for batch in generate_rows(total, batch_size):
            writer.writerows(batch)
            print(f"已写入 {len(batch)} 条数据")

if __name__ == "__main__":
    main()
