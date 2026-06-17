# lofty-quant

个人 A 股量化交易系统。当前项目已经完成项目骨架、TOML 配置加载、Loguru 日志配置、A 股日线研究版 DuckDB 数据层，以及 `Tushare -> raw CSV -> DuckDB/Parquet` 的交易日历和日线行情轻量 ETL 链路；因子、策略、回测和更多数据源后续按实际使用逐步实现。

## 技术栈

- Python 3.12
- uv
- tomllib + Pydantic Settings + Pydantic
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
├── scripts/                          # CLI/任务入口
├── src/quant/
│   ├── config.py                     # 统一配置加载
│   ├── logger.py                     # 统一日志配置
│   ├── data/                         # DuckDB schema、数据模型和查询入口
│   ├── etl/                          # 轻量 ETL fetch/load、数据源适配和存储工具
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

配置使用 TOML 作为主格式，统一从 `quant.config.load_config()` 读取。普通配置由标准库 `tomllib` 读取并递归合并，机密只通过 Pydantic Settings 从环境变量读取。

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
logger.info("系统启动")
```

如果需要写入模块专用日志，可以用 Loguru 的 `bind()` 绑定结构化字段。当前 `module="etl"` 会额外写入 ETL 专用日志文件：

```python
etl_logger = logger.bind(module="etl")
etl_logger.info("ETL 任务启动")
```

默认日志目录来自配置：

```toml
[paths]
log_dir = "log"
```

日志文件规则：

- 默认保存到项目根目录 `log/`
- 通用日志文件：`lofty-quant_YYYY-MM-DD.log`
- ETL 专用文件：`etl_YYYY-MM-DD.log`
- 单文件超过 10MB 自动分块
- 日期来自 logger 初始化时的 `{time:YYYY-MM-DD}`，适合当前脚本/任务型运行方式
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
- `etl_manifest`：兼容保留的旧 ETL 状态表, 当前业务流程不依赖它

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

日线行情 load 时先写月度 Parquet 文件：

```text
data/processed/ohlcv/year=2024/month=01/*.parquet
```

已结束年份可以通过独立归档命令合并为年文件：

```text
data/processed/ohlcv/year=2024/ohlcv_2024.parquet
```

归档成功后会删除对应月文件，避免 DuckDB 视图重复读取同一批行情数据。复权因子、每日指标、指数行情和因子仍按交易日期年月分区：

```text
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

## ETL 入口

ETL 使用 `scripts/run_etl.py` 作为轻量入口。当前不做 pipeline 框架和自动调度, 只保留 raw 落盘和 raw 加载两个阶段。

ETL 分为两个阶段：

```text
外部数据源 -> fetch -> data/raw -> load -> DuckDB 或 processed Parquet
```

阶段命令可以独立执行：

```bash
uv run python scripts/run_etl.py fetch trade-calendar \
  --source tushare \
  --exchange SSE \
  --start-date 20240101 \
  --end-date 20240131

uv run python scripts/run_etl.py load trade-calendar \
  --source tushare \
  --exchange SSE \
  --start-date 20240101 \
  --end-date 20240131

uv run python scripts/run_etl.py fetch daily-ohlcv \
  --source tushare \
  --exchange SSE \
  --start-date 20240102 \
  --end-date 20240131

uv run python scripts/run_etl.py load daily-ohlcv \
  --source tushare \
  --exchange SSE \
  --start-date 20240102 \
  --end-date 20240131
```

生命周期命令负责编排阶段：

```bash
uv run python scripts/run_etl.py backfill trade-calendar \
  --source tushare \
  --exchange SSE \
  --start-date 20200101 \
  --end-date 20260614

uv run python scripts/run_etl.py status trade-calendar \
  --source tushare

uv run python scripts/run_etl.py status daily-ohlcv \
  --source tushare

uv run python scripts/run_etl.py archive daily-ohlcv \
  --source tushare \
  --year 2025
```

生命周期约定：

- `fetch`：只连接外部数据源，只写 `data/raw`，不写 DuckDB，不写 processed。
- `load`：只读取 `data/raw`，清洗转换后写 DuckDB 或 processed。
- `backfill`：历史回填，必须显式传入日期范围，按 `fetch -> load` 执行。
- `archive`：将已结束年份的日线行情月文件合并为年文件，并删除对应月文件。
- `status`：直接从目标表或 processed 数据聚合当前真实状态。

当前已支持：

- `trade-calendar + tushare`：拉取 Tushare 交易日历，保存 raw CSV，加载到 DuckDB `dim_trade_calendar`，并通过 `status` 查询目标表真实状态。
- `daily-ohlcv + tushare`：读取本地交易日历中的开市日，逐日调用 Tushare 日线接口，每个交易日保存一个 raw CSV；load 时标准化字段并写入月度 processed Parquet；已结束年份可归档为年度 Parquet。

raw 层约定使用数据源接口返回的 `pandas.DataFrame` 原样保存为 CSV：

```text
data/raw/tushare/trade-calendar/trade-calendar_tushare.csv
data/raw/tushare/daily-ohlcv/year=2024/month=01/daily-ohlcv_tushare_20240102.csv
```

交易日历这类小维表使用单文件 raw；日线行情这类持续增长的数据集按年月分区, 每个交易日一个 raw CSV。raw 文件是 fetch 阶段的输入缓存，不代表当前完整数据状态；DuckDB 表或 processed Parquet 才是 load 后的事实源。

拉取 Tushare 日线行情前，需要先完成交易日历加载。日线接口会按 `dim_trade_calendar` 中的开市日逐日请求，并在请求之间固定等待 0.2 秒，避免超过每分钟 500 次。范围 fetch 会生成多个单日 raw 文件。

日线行情 processed 层约定：

```text
data/processed/ohlcv/year=2024/month=01/ohlcv_202401.parquet
data/processed/ohlcv/year=2024/ohlcv_2024.parquet
```

`load daily-ohlcv` 永远先写月文件；如果同一月文件已存在，会读取旧文件和新 raw 合并，并按 `(ts_code, trade_date)` 去重，新数据覆盖旧数据。`archive daily-ohlcv --year YYYY` 只允许归档已结束年份，会把该年份月文件合并到年文件，写入成功后删除月文件。不要长期同时保留同一年份的月文件和年文件，否则递归读取时会重复统计。

遇到未实现的数据集时, CLI 会返回中文错误：

```text
暂未实现数据集: dataset=..., source=...
```

暂不维护 ETL 状态表作为判断依据。`etl_manifest` 只做兼容保留，业务状态查询以目标数据实时聚合为准；交易日历从 `dim_trade_calendar` 聚合，日线行情从 `data/processed/ohlcv/**/*.parquet` 聚合。

## 常用命令

```bash
make install      # uv sync --all-extras
make etl-status   # uv run python scripts/run_etl.py status trade-calendar --source tushare
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
- 轻量 ETL 入口
- Tushare 交易日历 fetch/load/status 最小链路
- Tushare 日线行情 raw fetch、月度 load、年度 archive、status
- 基础测试覆盖

待实现：

- AkShare、MiniQMT 等更多数据源适配
- 日线行情缺失交易日检查和补数辅助命令
- 技术指标和因子 pipeline
- 策略基类和信号生成
- A 股回测撮合、组合、绩效指标

更完整的模块设计见 [lofty-quant-design.md](lofty-quant-design.md)。
