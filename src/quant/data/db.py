"""DuckDB connection and schema management for A-share research data."""

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
    """Mapping between a processed Parquet folder and its DuckDB view."""

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
    """Manage DuckDB connections, physical tables, and Parquet-backed views."""

    def __init__(self, db_path: Path, processed_dir: Path) -> None:
        self._db_path = db_path.expanduser().resolve()
        self._processed_dir = processed_dir.expanduser().resolve()
        self._conn: DuckDBPyConnection | None = None

    def connect(self) -> DuckDBPyConnection:
        """Open a DuckDB connection."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self._db_path))
        return self._conn

    def initialize(self) -> None:
        """Create physical schema and register any available Parquet views."""
        with self.session() as conn:
            self._create_tables(conn)
            self._register_parquet_views(conn)
            self._create_derived_views(conn)

    def close(self) -> None:
        """Close the active DuckDB connection if one exists."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def session(self) -> Generator[DuckDBPyConnection, None, None]:
        """Yield a DuckDB connection and close it after use."""
        conn = self.connect()
        try:
            yield conn
        finally:
            self.close()

    def _create_tables(self, conn: DuckDBPyConnection) -> None:
        """Create small physical dimension and metadata tables."""
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
        """Register processed Parquet datasets as DuckDB views when files exist."""
        for dataset in PARQUET_DATASETS:
            parquet_glob = self._processed_dir / dataset.folder_name / "**" / "*.parquet"
            if not list((self._processed_dir / dataset.folder_name).glob("**/*.parquet")):
                logger.debug("Skipping missing Parquet dataset {}", dataset.folder_name)
                continue

            conn.execute(
                f"""
                CREATE OR REPLACE VIEW {dataset.view_name} AS
                SELECT * FROM read_parquet('{_duckdb_path(parquet_glob)}', hive_partitioning=true)
                """
            )
            logger.info("Registered DuckDB view {}", dataset.view_name)

    def _create_derived_views(self, conn: DuckDBPyConnection) -> None:
        """Create derived research views from registered source views."""
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
        logger.info("Registered DuckDB view v_daily_adj")


def _relation_exists(conn: DuckDBPyConnection, relation_name: str) -> bool:
    """Return whether a DuckDB table or view exists."""
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
    """Format a filesystem path for DuckDB SQL string literals."""
    return path.as_posix().replace("'", "''")
