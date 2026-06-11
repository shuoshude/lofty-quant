# lofty-quant

个人 A 股量化交易系统。当前项目处于基础设施搭建阶段，已经完成项目骨架、TOML 配置加载、Loguru 日志配置，以及 A 股日线研究版 DuckDB 数据层；ETL、因子、策略、回测等模块后续按设计文档逐步实现。

## 技术栈

- Python 3.12
- uv
- Dynaconf + Pydantic
- Loguru
- DuckDB / Parquet / Polars
- pytest / ruff / mypy

## 项目结构

```text
.
├── config/
│   ├── settings.toml                 # 默认 TOML 配置
│   └── settings.local.example.toml   # 本地覆盖配置模板
├── data/
│   ├── raw/                          # 原始数据目录
│   ├── processed/                    # 处理后数据目录
│   └── db/                           # DuckDB 数据库目录
├── log/                              # 运行日志目录，默认不提交
├── notebooks/                        # 研究 notebook
├── scripts/                          # CLI/任务入口占位
├── src/quant/
│   ├── config.py                     # 统一配置加载
│   ├── logger.py                     # 统一日志配置
│   ├── data/                         # DuckDB schema、数据模型和查询入口
│   ├── etl/                          # ETL 占位
│   ├── features/                     # 因子工程占位
│   ├── strategy/                     # 策略层占位
│   ├── backtest/                     # 回测引擎占位
│   ├── risk/                         # 风控模块占位
│   └── analysis/                     # 分析输出占位
└── tests/                            # 单元测试
```

## 安装

项目使用 `uv` 管理依赖。

```bash
uv sync --all-extras
```

运行当前入口：

```bash
uv run python main.py
```

## 配置

配置使用 TOML 作为主格式，统一从 `quant.config.load_config()` 读取。普通配置放在文件里，机密只放环境变量。

加载顺序：

```text
config/settings.toml
→ config/settings.<LOFTY_QUANT_ENV>.toml
→ config/settings.local.toml
→ LOFTY_QUANT__SECRETS__... 环境变量
```

默认环境为 `default`。如果要启用某个环境覆盖文件，例如 `config/settings.research.toml`：

```bash
export LOFTY_QUANT_ENV=research
```

本地覆盖配置：

```bash
cp config/settings.local.example.toml config/settings.local.toml
```

`config/settings.local.toml` 已被 `.gitignore` 忽略，适合放本机路径等非机密覆盖项。

机密环境变量示例：

```bash
export LOFTY_QUANT__SECRETS__TUSHARE_TOKEN=your-token
export LOFTY_QUANT__SECRETS__AKSHARE_TOKEN=your-token
```

代码中读取配置：

```python
from quant.config import load_config

config = load_config()

raw_dir = config.paths.raw_dir
database_path = config.paths.database_path
tushare_token = config.secrets.tushare_token
```

## 日志

日志使用 Loguru。`loguru.logger` 本身是全局单例，项目只提供 `setup_logger()` 负责统一配置。

```python
from loguru import logger

from quant.logger import setup_logger

setup_logger()
logger.info("system started")
```

默认日志目录来自配置：

```toml
[paths]
log_dir = "log"
```

日志文件规则：

- 默认保存到项目根目录 `log/`
- 文件名按天：`lofty-quant_YYYY-MM-DD.log`
- 跨天自动切换文件
- 单文件超过 10MB 自动分块
- 日志格式包含时间戳、日志级别、模块/函数/行号和消息

## 数据层 / DuckDB Schema

数据层采用混合模式：小型维表和元数据表写入 DuckDB，大型时序数据以 Parquet 保存，再由 DuckDB 注册成视图查询。

核心文件：

- `src/quant/data/schemas.py`：Pydantic 数据契约，校验 `ts_code`、OHLC、成交量、涨跌停状态、财务公告日期等。
- `src/quant/data/db.py`：DuckDB 连接和 schema 初始化，创建实体表并注册 Parquet 视图。
- `src/quant/data/repository.py`：唯一公开查询入口，业务代码不要在其他模块直接写 SQL。

### 初始化数据库

```python
from quant.config import load_config
from quant.data.db import DuckDBManager

config = load_config()

manager = DuckDBManager(
    db_path=config.paths.database_path,
    processed_dir=config.paths.processed_dir,
)
manager.initialize()
```

`initialize()` 会创建以下 DuckDB 实体表：

- `dim_security`：股票主数据
- `dim_trade_calendar`：交易日历
- `etl_manifest`：ETL 加载记录

如果 `data/processed/` 下存在对应 Parquet 文件，还会自动注册视图：

```text
data/processed/ohlcv/         -> v_daily_ohlcv
data/processed/adj_factor/    -> v_adj_factor
data/processed/daily_basic/   -> v_daily_basic
data/processed/index_daily/   -> v_index_daily
data/processed/fundamental/   -> v_fundamental
data/processed/factors/       -> v_factors
```

当 `v_daily_ohlcv` 和 `v_adj_factor` 同时存在时，会额外创建 `v_daily_adj`，用于查询复权价格。

### Parquet 分区约定

日线行情、复权因子、每日指标、指数行情和因子按交易日期分区：

```text
data/processed/ohlcv/year=2024/month=01/*.parquet
data/processed/adj_factor/year=2024/month=01/*.parquet
data/processed/daily_basic/year=2024/month=01/*.parquet
data/processed/index_daily/year=2024/month=01/*.parquet
data/processed/factors/year=2024/month=01/*.parquet
```

财务数据按公告日期 `ann_date` 分区，避免回测时误用未来数据：

```text
data/processed/fundamental/year=2024/month=04/*.parquet
```

### 查询数据

所有查询建议通过 `QuantRepository`：

```python
from datetime import date

from quant.config import load_config
from quant.data.db import DuckDBManager
from quant.data.repository import QuantRepository

config = load_config()
manager = DuckDBManager(config.paths.database_path, config.paths.processed_dir)
manager.initialize()

with manager.session() as conn:
    repo = QuantRepository(conn)

    bars = repo.get_daily_bars(
        "000001.SZ",
        date(2024, 1, 1),
        date(2024, 1, 31),
        adjusted=True,
    )

    factors = repo.get_factors(
        date(2024, 1, 31),
        ["momentum_20d"],
        factor_version="v1",
    )
```

常用查询接口：

- `get_daily_bars(ts_code, start, end, adjusted=True)`：查询单只股票日线，默认返回复权视图。
- `get_cross_section(trade_date, fields, exclude_suspended=False)`：查询某日截面，可选择排除停牌股票。
- `get_factors(trade_date, factor_names, factor_version=None)`：查询因子值。
- `get_trade_calendar(start, end, exchange="SSE")`：查询交易日历。

## 常用命令

```bash
make install      # uv sync --all-extras
make test         # uv run pytest
make lint         # uv run ruff check src/ tests/
make format       # uv run ruff format . && uv run ruff check --fix src/ tests/
make typecheck    # uv run mypy src/
make notebook     # uv run jupyter lab --no-browser
```

也可以直接运行：

```bash
uv run pytest
uv run ruff check src/ tests/ scripts/ main.py
uv run mypy src/
```

## 当前状态

已完成：

- 项目目录骨架
- TOML 多层配置加载
- 环境变量机密读取
- Loguru 日志配置
- A 股日线研究版 DuckDB schema
- Pydantic 数据模型
- Repository 查询入口
- 基础测试覆盖

待实现：

- ETL 读取、清洗、转换、加载
- 技术指标和因子 pipeline
- 策略基类和信号生成
- A 股回测撮合、组合、绩效指标

更完整的模块设计见 [lofty-quant-design.md](lofty-quant-design.md)。
