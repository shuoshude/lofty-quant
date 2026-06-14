"""DuckDB 连接管理和 schema 定义。

设计原则:
- 大型不可变时序数据保存为分区 Parquet 文件, 并通过 read_parquet 注册为 DuckDB 视图。
- 小型可变维表和元数据保存为带主键的 DuckDB 原生表。

所有 SQL 查询应通过 src/quant/data/repository.py 进入。本模块只负责连接管理,
schema 创建和视图注册。
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection
from loguru import logger


@dataclass(frozen=True)
class ParquetDataset:
    """处理后 Parquet 目录与 DuckDB 视图的映射。"""

    view_name: str
    folder_name: str


PARQUET_DATASETS = (
    ParquetDataset("v_daily_ohlcv", "ohlcv"),
    ParquetDataset("v_adj_factor", "adj_factor"),
    ParquetDataset("v_daily_basic", "daily_basic"),
    ParquetDataset("v_index_daily", "index_daily"),
    ParquetDataset("v_fundamental", "fundamental"),
    ParquetDataset("v_factors", "factors"),
)


class DuckDBManager:
    """管理 DuckDB 连接, 实体表和 Parquet 视图。"""

    def __init__(self, db_path: Path, processed_dir: Path) -> None:
        self._db_path = db_path.expanduser().resolve()
        self._processed_dir = processed_dir.expanduser().resolve()
        self._conn: DuckDBPyConnection | None = None

    def connect(self) -> DuckDBPyConnection:
        """打开 DuckDB 连接。"""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self._db_path))
        return self._conn

    def initialize(self) -> None:
        """创建实体 schema 并注册可用的 Parquet 视图。"""
        with self.session() as conn:
            self._create_tables(conn)
            self._register_parquet_views(conn)
            self._create_derived_views(conn)

    def close(self) -> None:
        """关闭当前活动的 DuckDB 连接。"""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def session(self) -> Generator[DuckDBPyConnection, None, None]:
        """提供 DuckDB 连接并在使用后关闭。"""
        conn = self.connect()
        try:
            yield conn
        finally:
            self.close()

    def _create_tables(self, conn: DuckDBPyConnection) -> None:
        """创建小型实体维表和元数据表。"""
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_security (
                ts_code VARCHAR PRIMARY KEY,
                symbol VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                market VARCHAR,
                list_date DATE,
                delist_date DATE,
                is_active BOOLEAN DEFAULT TRUE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_trade_calendar (
                exchange VARCHAR NOT NULL,
                cal_date DATE NOT NULL,
                is_open BOOLEAN NOT NULL,
                pretrade_date DATE,
                PRIMARY KEY (exchange, cal_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_manifest (
                dataset VARCHAR NOT NULL,
                trade_date DATE,
                source VARCHAR NOT NULL,
                version VARCHAR NOT NULL,
                row_count BIGINT NOT NULL,
                loaded_at TIMESTAMP NOT NULL,
                PRIMARY KEY (dataset, trade_date, source, version)
            )
            """
        )

    def _register_parquet_views(self, conn: DuckDBPyConnection) -> None:
        """当 Parquet 文件存在时, 注册处理后数据集为 DuckDB 视图。"""
        for dataset in PARQUET_DATASETS:
            parquet_glob = self._processed_dir / dataset.folder_name / "**" / "*.parquet"
            if not list((self._processed_dir / dataset.folder_name).glob("**/*.parquet")):
                logger.debug("跳过缺失的 Parquet 数据集 {}", dataset.folder_name)
                continue

            conn.execute(
                f"""
                CREATE OR REPLACE VIEW {dataset.view_name} AS
                SELECT *
                FROM read_parquet(
                    '{_duckdb_path(parquet_glob)}',
                    union_by_name=true
                )
                """
            )
            logger.info("已注册 DuckDB 视图 {}", dataset.view_name)

    def _create_derived_views(self, conn: DuckDBPyConnection) -> None:
        """基于已注册源视图创建衍生研究视图。"""
        if not (_relation_exists(conn, "v_daily_ohlcv") and _relation_exists(conn, "v_adj_factor")):
            return

        conn.execute(
            """
            CREATE OR REPLACE VIEW v_daily_adj AS
            SELECT
                o.ts_code,
                o.trade_date,
                o.open,
                o.high,
                o.low,
                o.close,
                a.adj_factor,
                o.open * a.adj_factor AS adj_open,
                o.high * a.adj_factor AS adj_high,
                o.low * a.adj_factor AS adj_low,
                o.close * a.adj_factor AS adj_close,
                o.volume,
                o.amount,
                o.is_suspended,
                o.is_st,
                o.limit_status
            FROM v_daily_ohlcv o
            LEFT JOIN v_adj_factor a
              ON o.ts_code = a.ts_code
             AND o.trade_date = a.trade_date
            """
        )
        logger.info("已注册 DuckDB 视图 v_daily_adj")


def _relation_exists(conn: DuckDBPyConnection, relation_name: str) -> bool:
    """返回 DuckDB 表或视图是否存在。"""
    result = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [relation_name],
    ).fetchone()
    return bool(result and result[0])


def _duckdb_path(path: Path) -> str:
    """将文件系统路径格式化为 DuckDB SQL 字符串字面量。"""
    return path.as_posix().replace("'", "''")
