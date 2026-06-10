# A股量化系统 — 项目设计文档

> 技术栈：Python 3.12 · uv · DuckDB · Parquet · Polars · pytest · ruff  
> 开发工具：Claude Code · Codex · VS Code

---

## 目录结构

``` markdown

quant-ashare/
├── AGENT.md                        # Claude Code / Codex 项目规则（必须）
├── pyproject.toml                   # uv 依赖管理 + 工具配置
├── .python-version                  # 3.12
├── .env.example                     # 环境变量模板
├── Makefile                         # 常用命令快捷键
│
├── data/
│   ├── raw/                         # 原始 CSV（只读，绝不修改）
│   │   ├── daily/                   # 日线行情  000001.SZ_daily.csv
│   │   ├── minute/                  # 分钟线
│   │   ├── fundamental/             # 财务数据（资产负债表、利润表、现金流）
│   │   └── index/                   # 指数行情（沪深300、中证500等）
│   │
│   ├── processed/                   # 清洗后 Parquet（按日期分区）
│   │   ├── ohlcv/
│   │   │   └── year=2024/month=01/ *.parquet
│   │   ├── fundamental/
│   │   └── factors/                 # 计算好的因子缓存
│   │
│   └── db/
│       └── quant.duckdb             # DuckDB 数据库文件
│
├── src/
│   └── quant/
│       ├── __init__.py
│       │
│       ├── etl/                     # ETL 管道（数据流入口）
│       │   ├── __init__.py
│       │   ├── ingestion.py         # CSV 读取 + schema 校验
│       │   ├── cleaner.py           # A股特有清洗：停复牌/涨跌停/ST
│       │   ├── transformer.py       # 转换为 Parquet + 列类型优化
│       │   └── loader.py            # 注册 DuckDB 视图 + 连接管理
│       │
│       ├── data/                    # 数据访问层（DAO）
│       │   ├── __init__.py
│       │   ├── db.py                # DuckDB 连接池 + context manager
│       │   ├── repository.py        # 数据查询接口（唯一的查询入口）
│       │   └── schemas.py           # Pydantic 数据模型 + 校验规则
│       │
│       ├── features/                # 特征工程
│       │   ├── __init__.py
│       │   ├── technical.py         # 技术指标（MA/RSI/MACD/布林带/ATR）
│       │   ├── fundamental.py       # 基本面因子（PE/PB/ROE/现金流）
│       │   ├── alternative.py       # 另类因子（换手率/资金流向）
│       │   └── pipeline.py          # 因子计算管道 + 缓存
│       │
│       ├── strategy/                # 策略层
│       │   ├── __init__.py
│       │   ├── base.py              # Strategy 抽象基类
│       │   ├── signals.py           # 信号生成工具函数
│       │   └── alpha.py             # Alpha 因子框架
│       │
│       ├── backtest/                # 回测引擎
│       │   ├── __init__.py
│       │   ├── engine.py            # 事件驱动主引擎
│       │   ├── portfolio.py         # 持仓管理
│       │   ├── broker.py            # A股模拟撮合（含涨跌停/T+1）
│       │   └── metrics.py           # 绩效指标（夏普/回撤/卡玛）
│       │
│       ├── risk/                    # 风险管理
│       │   ├── __init__.py
│       │   ├── limits.py            # 风险限制规则
│       │   └── position.py          # 仓位计算（Kelly/等权/风险平价）
│       │
│       └── analysis/                # 分析输出
│           ├── __init__.py
│           ├── report.py            # 绩效报告生成
│           └── visualization.py     # Plotly 可视化
│
├── notebooks/
│   ├── 01_data_exploration.ipynb    # 数据探索
│   ├── 02_feature_analysis.ipynb    # 因子分析（IC/ICIR/分组收益）
│   └── 03_strategy_research.ipynb   # 策略研究
│
├── tests/
│   ├── conftest.py                  # pytest fixtures（含测试用 DuckDB）
│   ├── test_etl/
│   │   ├── test_ingestion.py
│   │   ├── test_cleaner.py
│   │   └── test_transformer.py
│   ├── test_features/
│   │   └── test_technical.py
│   └── test_backtest/
│       ├── test_broker.py           # 重点测试涨跌停撮合逻辑
│       └── test_metrics.py
│
└── scripts/
    ├── run_etl.py                   # ETL 入口：python scripts/run_etl.py --date 20240101
    └── run_backtest.py              # 回测入口
```

---

## 关键模块接口

### `src/quant/data/schemas.py` — 数据模型

```python
"""A 股数据 Pydantic 模型。所有数据校验的唯一入口。"""

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, field_validator


TsCode = str  # 格式：000001.SZ / 600000.SH

class OHLCVRecord(BaseModel):
    """日线行情记录。"""
    ts_code: TsCode
    trade_date: date
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)          # 成交量（手）
    amount: float = Field(ge=0)          # 成交额（元）
    adj_factor: float = Field(default=1.0, gt=0)  # 复权因子
    is_suspended: bool = Field(default=False)      # 是否停牌
    is_st: bool = Field(default=False)             # 是否 ST
    limit_status: Literal["up", "down", "none"] = "none"  # 涨跌停状态

    @field_validator("ts_code")
    @classmethod
    def validate_ts_code(cls, v: str) -> str:
        if not (v.endswith(".SZ") or v.endswith(".SH") or v.endswith(".BJ")):
            raise ValueError(f"无效的 ts_code 格式: {v}，应为 000001.SZ 或 600000.SH")
        return v

    @field_validator("high")
    @classmethod
    def high_gte_low(cls, v: float, info: any) -> float:
        if "low" in info.data and v < info.data["low"]:
            raise ValueError("high 不能低于 low")
        return v


class FundamentalRecord(BaseModel):
    """基本面数据记录（季频）。"""
    ts_code: TsCode
    ann_date: date       # 公告日期
    report_date: date    # 报告期
    pe_ttm: float | None = None
    pb: float | None = None
    ps_ttm: float | None = None
    roe: float | None = None
    revenue_yoy: float | None = None  # 营收同比
```

---

### `src/quant/data/db.py` — DuckDB 连接管理

```python
"""DuckDB 连接管理。所有 SQL 查询必须通过此模块进行。"""

from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb
from loguru import logger


class DuckDBManager:
    """DuckDB 连接管理器，自动注册 Parquet 视图。"""

    def __init__(self, db_path: Path, processed_dir: Path) -> None:
        self._db_path = db_path
        self._processed_dir = processed_dir
        self._conn: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """建立连接并注册所有 Parquet 视图。"""
        self._conn = duckdb.connect(str(self._db_path))
        self._register_views()
        return self._conn

    def _register_views(self) -> None:
        """将 Parquet 文件注册为 DuckDB 视图（无需 ETL 导入）。"""
        assert self._conn is not None

        ohlcv_path = self._processed_dir / "ohlcv" / "**" / "*.parquet"
        if list(self._processed_dir.glob("ohlcv/**/*.parquet")):
            self._conn.execute(f"""
                CREATE OR REPLACE VIEW v_ohlcv AS
                SELECT * FROM read_parquet('{ohlcv_path}', hive_partitioning=true)
            """)
            logger.info("已注册视图 v_ohlcv")

        factor_path = self._processed_dir / "factors" / "**" / "*.parquet"
        if list(self._processed_dir.glob("factors/**/*.parquet")):
            self._conn.execute(f"""
                CREATE OR REPLACE VIEW v_factors AS
                SELECT * FROM read_parquet('{factor_path}', hive_partitioning=true)
            """)
            logger.info("已注册视图 v_factors")

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    def session(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Context manager，自动关闭连接。"""
        conn = self.connect()
        try:
            yield conn
        finally:
            self.close()
```

## DuckDB Schema

```sql
-- 执行位置：src/quant/data/db.py 的 _create_tables()

-- 日线行情表（ETL 写入）
CREATE TABLE IF NOT EXISTS daily_ohlcv (
    ts_code     VARCHAR NOT NULL,   -- e.g. 000001.SZ
    trade_date  DATE    NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      DOUBLE,             -- 成交量（手）
    amount      DOUBLE,             -- 成交额（元）
    adj_factor  DOUBLE DEFAULT 1.0, -- 复权因子
    is_suspended BOOLEAN DEFAULT FALSE,
    is_st       BOOLEAN DEFAULT FALSE,
    limit_status VARCHAR DEFAULT 'none',  -- up/down/none
    PRIMARY KEY (ts_code, trade_date)
);

-- 复权价格视图（后复权）
CREATE OR REPLACE VIEW v_adj_close AS
SELECT
    ts_code,
    trade_date,
    close * adj_factor AS adj_close,
    volume,
    amount,
    limit_status,
    is_suspended
FROM daily_ohlcv;

-- 因子表
CREATE TABLE IF NOT EXISTS factors (
    ts_code     VARCHAR NOT NULL,
    trade_date  DATE    NOT NULL,
    factor_name VARCHAR NOT NULL,
    value       DOUBLE,
    PRIMARY KEY (ts_code, trade_date, factor_name)
);

-- 常用查询示例
-- 查询某股票复权收益率序列：
-- SELECT trade_date, adj_close / LAG(adj_close) OVER (PARTITION BY ts_code ORDER BY trade_date) - 1 AS ret
-- FROM v_adj_close WHERE ts_code = '000001.SZ' ORDER BY trade_date;

-- 查询截面因子数据：
-- SELECT * FROM factors WHERE trade_date = '2024-01-31' AND factor_name = 'momentum_20d';
```

---

## 开发优先级建议

开发顺序建议按以下优先级进行，每个阶段都有可运行的测试：

1. `data/schemas.py` → 定义数据契约，其他模块依赖
2. `data/db.py` → DuckDB 连接管理
3. `etl/ingestion.py` → CSV 读取 + 校验
4. `etl/cleaner.py` → A 股特有清洗逻辑
5. `etl/transformer.py` → 写 Parquet
6. `etl/loader.py` → 注册 DuckDB 视图
7. `features/technical.py` → 基础技术指标
8. `backtest/broker.py` → A 股撮合逻辑（最关键，需要详细测试）
9. `backtest/engine.py` → 事件驱动引擎
10. `backtest/metrics.py` → 绩效指标
11. `strategy/base.py` → 策略基类
12. `analysis/report.py` → 绩效报告
