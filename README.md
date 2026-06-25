# lofty-quant

个人 A 股量化交易系统。已完成项目骨架、多层 TOML 配置、Loguru 日志,以及 `Tushare -> raw CSV -> DuckDB/Parquet` 的交易日历和日线行情轻量 ETL 链路(实测可拉取 13 年交易日历 + 4 年多全市场日线);因子、策略、回测等量化模块按实际使用逐步实现。

## 技术栈

- Python 3.12
- uv(包管理,禁止 pip)
- tomllib + Pydantic v2 + Pydantic Settings(配置)
- Loguru(日志)
- DuckDB + Parquet + Pandas(存储与数据处理)
- pytest / ruff / mypy(质量工具)

## 项目结构

```text
.
├── config/
│   ├── settings.toml                 # 默认 TOML 配置
│   └── settings.local.example.toml   # 本地覆盖配置模板
├── data/
│   ├── raw/                          # 原始数据目录(fetch 写入)
│   ├── processed/                    # 处理后数据目录(load 写入)
│   └── db/                           # DuckDB 数据库目录
├── log/                              # 运行日志目录(默认不提交)
├── notebooks/                        # 研究 notebook
├── scripts/
│   ├── run_etl.py                    # ETL CLI 入口
│   └── run_backtest.py               # 回测入口(占位)
├── src/quant/
│   ├── config.py                     # 统一配置加载
│   ├── logger.py                     # 统一日志配置
│   ├── data/                         # DuckDB schema、数据契约和查询入口
│   ├── etl/                          # 轻量 ETL(fetch/load/processed/storage)
│   │   └── sources/tushare_source.py # Tushare 数据源适配器
│   ├── features/                     # 因子工程(占位)
│   ├── strategy/                     # 策略层(占位)
│   ├── backtest/                     # 回测引擎(占位)
│   ├── risk/                         # 风控模块(占位)
│   └── analysis/                     # 分析输出(占位)
├── tests/                            # 单元测试(87 个,覆盖率 94%)
└── lofty-quant-design.md             # 模块设计文档
```

## 安装

项目使用 `uv` 管理依赖。

```bash
uv sync --all-extras
```

## 配置

配置使用 TOML 作为主格式,统一从 `quant.config.load_config()` 读取。普通配置由标准库 `tomllib` 读取并递归合并,机密只通过 Pydantic Settings 从环境变量读取。

加载顺序(后者覆盖前者):

```text
config/settings.toml
→ config/settings.<LOFTY_QUANT_ENV>.toml
→ config/settings.local.toml
→ LOFTY_QUANT__SECRETS__... 环境变量
```

默认环境为 `default`。如要启用某个环境覆盖文件(例如 `config/settings.research.toml`):

```bash
export LOFTY_QUANT_ENV=research
```

本地覆盖配置:

```bash
cp config/settings.local.example.toml config/settings.local.toml
```

`config/settings.local.toml` 已被 `.gitignore` 忽略,适合放本机路径等非机密覆盖项。

机密环境变量示例:

```bash
export LOFTY_QUANT__SECRETS__TUSHARE_TOKEN=your-token
export LOFTY_QUANT__SECRETS__AKSHARE_TOKEN=your-token
```

代码中读取配置:

```python
from quant.config import load_config

config = load_config()

raw_dir = config.paths.raw_dir
database_path = config.paths.database_path
tushare_token = config.secrets.tushare_token
```

## 日志

日志使用 Loguru。`loguru.logger` 本身是全局单例,项目只提供 `setup_logger()` 负责统一配置。

```python
from loguru import logger

from quant.logger import setup_logger

setup_logger()
logger.info("系统启动")
```

如需写入模块专用日志,用 Loguru 的 `bind()` 绑定结构化字段。当前 `module="etl"` 会额外写入 ETL 专用日志文件,其中 ERROR 及以上级别会单独写入错误日志:

```python
etl_logger = logger.bind(module="etl")
etl_logger.info("ETL 任务启动")
```

日志文件规则:

- 默认保存到配置中的 `log_dir`(默认 `log/`)
- 通用日志文件:`lofty-quant_YYYY-MM-DD.log`
- ETL 普通日志文件:`etl_YYYY-MM-DD.log`
- ETL 错误日志文件:`etl_error_YYYY-MM-DD.log`
- 单文件超过 10MB 自动分块
- 日志格式包含时间戳、日志级别、模块/函数/行号和消息

## 数据层 / DuckDB Schema

数据层采用混合存储模式:小型维表和元数据写入 DuckDB 原生表,大型时序数据以 Parquet 保存,再由 DuckDB 注册成视图查询。

核心文件:

- `src/quant/data/schemas.py`:Pydantic 数据契约,校验 `ts_code`、OHLC 价格区间、成交量、涨跌停状态等。日线 load 末尾会用 `DailyOHLCVRecord` 对每行做最终校验。
- `src/quant/data/db.py`:DuckDB 连接和 schema 初始化,创建实体表并注册 Parquet 视图。
- `src/quant/data/repository.py`:唯一公开查询入口,业务代码不要在其他模块直接写 SQL。

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

`initialize()` 会创建以下 DuckDB 实体表:

- `dim_security`:股票主数据,由 Tushare `stock_basic` 快照全量覆盖写入
- `dim_trade_calendar`:交易日历
- `etl_manifest`:兼容保留的旧 ETL 状态表,当前业务流程不写入也不依赖它

如果 `data/processed/` 下存在对应 Parquet 文件,还会自动注册视图:

```text
data/processed/ohlcv/         -> v_daily_ohlcv
data/processed/adj_factor/    -> v_adj_factor
data/processed/daily_basic/   -> v_daily_basic
data/processed/index_daily/   -> v_index_daily
data/processed/fundamental/   -> v_fundamental
data/processed/factors/       -> v_factors
```

当 `v_daily_ohlcv` 和 `v_adj_factor` 同时存在时,会额外创建:

- `v_daily_hfq`:后复权价格,按 `raw_price * cumulative_factor` 计算
- `v_daily_qfq_latest`:展示用最新口径前复权价格
- `v_daily_adj`:兼容旧查询的 alias,新代码不再依赖它

### Parquet 分区约定

日线行情 load 时按月写入 Parquet 文件:

```text
data/processed/ohlcv/year=2024/month=01/ohlcv_202401.parquet
```

已结束年份可以通过独立归档命令合并为年文件:

```text
data/processed/ohlcv/year=2024/ohlcv_2024.parquet
```

归档成功后会删除对应月文件,避免 DuckDB 视图重复读取同一批行情数据。其他数据集(复权因子、每日指标、指数行情、因子)按交易日期年月分区:

```text
data/processed/adj_factor/year=2024/month=01/adj_factor_202401.parquet
data/processed/daily_basic/year=2024/month=01/daily_basic_202401.parquet
data/processed/index_daily/year=2024/month=01/*.parquet
data/processed/factors/year=2024/month=01/*.parquet
```

财务数据按公告日期 `ann_date` 分区,避免回测时误用未来数据:

```text
data/processed/fundamental/year=2024/month=04/*.parquet
```

### 查询数据

所有查询建议通过 `QuantRepository`:

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
        adjustment="none",
    )

    trade_dates = repo.get_open_trade_dates(
        date(2024, 1, 1),
        date(2024, 1, 31),
        exchange="SSE",
    )
```

常用查询接口:

- `get_daily_bars(ts_code, start, end, adjustment="none", as_of_date=None)`:查询单只股票日线。`adjustment="none"` 返回未复权;`"hfq"` 返回后复权;`"qfq"` 返回前复权,默认使用查询结束日作为 `as_of_date`,不会返回 `as_of_date` 之后的行情。
- `get_cross_section(trade_date, fields, exclude_suspended=False)`:查询某日截面,可选择排除停牌股票。
- `get_factors(trade_date, factor_names, factor_version=None)`:查询因子值(依赖 `v_factors` 视图,需先计算并加载因子)。
- `get_trade_calendar(start, end, exchange="SSE")`:查询交易日历记录。
- `get_open_trade_dates(start, end, exchange="SSE")`:查询指定交易所开市日列表。

## ETL 入口

ETL 使用 `scripts/run_etl.py` 作为轻量入口。不做 pipeline 框架和自动调度,只保留 raw 落盘和 raw 加载两个阶段。

ETL 分为两个阶段:

```text
外部数据源 -> fetch -> data/raw -> load -> DuckDB 或 processed Parquet
```

阶段命令可以独立执行。ETL 使用 Tushare `SSE` 交易日历作为本地统一 A 股开闭市日历,脚本入口不再暴露交易所参数。

```bash
uv run python scripts/run_etl.py fetch trade-calendar \
  --source tushare \
  --start-date 20240101 \
  --end-date 20240131

uv run python scripts/run_etl.py load trade-calendar \
  --source tushare \
  --start-date 20240101 \
  --end-date 20240131

uv run python scripts/run_etl.py fetch daily-ohlcv \
  --source tushare \
  --start-date 20240102 \
  --end-date 20240131

uv run python scripts/run_etl.py load daily-ohlcv \
  --source tushare \
  --start-date 20240102 \
  --end-date 20240131

uv run python scripts/run_etl.py fetch adj-factor \
  --source tushare \
  --start-date 20240102 \
  --end-date 20240131

uv run python scripts/run_etl.py load adj-factor \
  --source tushare \
  --start-date 20240102 \
  --end-date 20240131

uv run python scripts/run_etl.py fetch daily-basic \
  --source tushare \
  --start-date 20240102 \
  --end-date 20240131

uv run python scripts/run_etl.py load daily-basic \
  --source tushare \
  --start-date 20240102 \
  --end-date 20240131

uv run python scripts/run_etl.py fetch stock-st \
  --source tushare \
  --start-date 20240102 \
  --end-date 20240131

uv run python scripts/run_etl.py fetch stk-limit \
  --source tushare \
  --start-date 20240102 \
  --end-date 20240131

uv run python scripts/run_etl.py fetch suspend-d \
  --source tushare \
  --start-date 20240102 \
  --end-date 20240131

uv run python scripts/run_etl.py fetch stock-basic \
  --source tushare

uv run python scripts/run_etl.py load stock-basic \
  --source tushare
```

生命周期命令负责编排阶段:

```bash
uv run python scripts/run_etl.py backfill trade-calendar \
  --source tushare \
  --start-date 20130101 \
  --end-date 20260617

uv run python scripts/run_etl.py status trade-calendar --source tushare

uv run python scripts/run_etl.py status daily-ohlcv --source tushare

uv run python scripts/run_etl.py status adj-factor --source tushare

uv run python scripts/run_etl.py status daily-basic --source tushare

uv run python scripts/run_etl.py status stock-basic --source tushare

uv run python scripts/run_etl.py archive daily-ohlcv \
  --source tushare \
  --year 2023
```

生命周期约定:

- `fetch`:只连接外部数据源,只写 `data/raw`,不写 DuckDB,不写 processed。
- `load`:只读取 `data/raw`,清洗转换后写 DuckDB 或 processed。
- `backfill`:历史回填,按 `fetch -> load` 执行。除 `stock-basic` 这类快照型数据集外,通常需要显式传入日期范围。
- `archive`:将已结束年份的日线行情月文件合并为年文件,并删除对应月文件。
- `status`:直接从目标表或 processed 数据实时聚合当前真实状态。

### 已支持的数据集

- **trade-calendar + tushare**:拉取 Tushare 交易日历,保存 raw CSV,加载到 DuckDB `dim_trade_calendar`,并通过 `status` 查询目标表真实状态。
- **daily-ohlcv + tushare**:读取本地交易日历中的开市日,逐日调用 Tushare 日线接口,每个交易日保存一个 raw CSV;load 时会同时读取同日 `stock-st`, `stk-limit`, `suspend-d` raw 来生成 ST、停牌和涨跌停状态,再写入月度 processed Parquet;已结束年份可归档为年度 Parquet。
- **adj-factor + tushare**:读取本地交易日历中的开市日,逐日调用 Tushare `adj_factor` 接口,每个交易日保存一个 raw CSV;load 时把 Tushare `adj_factor` 标准化为项目字段 `cumulative_factor`,再写入月度 processed Parquet。
- **daily-basic + tushare**:读取本地交易日历中的开市日,逐日调用 Tushare `daily_basic` 接口,每个交易日保存一个 raw CSV;load 时按 Tushare 官方每日指标字段约定标准化并做 Pydantic 契约校验,再写入月度 processed Parquet。
- **stock-basic + tushare**:依次拉取 Tushare `stock_basic(list_status="L/D/P")`,合并为单个 raw CSV;load 时按接口字段选择列并全量覆盖 DuckDB `dim_security`,不做派生字段和契约校验。
- **stock-st / stk-limit / suspend-d + tushare**:读取本地交易日历中的开市日,逐日调用 Tushare `stock_st`, `stk_limit`, `suspend_d` 接口,只保存 raw CSV;不做 load、processed、DuckDB 视图或数据契约校验。

### raw 层约定

raw 层按数据集使用不同布局:

```text
# 单文件维表
data/raw/tushare/trade-calendar/trade-calendar_tushare.csv
data/raw/tushare/stock-basic/stock-basic_tushare.csv

# 按交易日分文件(每个交易日一个 raw CSV)
data/raw/tushare/daily-ohlcv/year=2024/month=01/daily-ohlcv_tushare_20240102.csv
data/raw/tushare/adj-factor/year=2024/month=01/adj-factor_tushare_20240102.csv
data/raw/tushare/daily-basic/year=2024/month=01/daily-basic_tushare_20240102.csv
data/raw/tushare/stock-st/year=2024/month=01/stock-st_tushare_20240102.csv
data/raw/tushare/stk-limit/year=2024/month=01/stk-limit_tushare_20240102.csv
data/raw/tushare/suspend-d/year=2024/month=01/suspend-d_tushare_20240102.csv
```

raw 文件是 fetch 阶段的输入缓存,不代表当前完整数据状态;DuckDB 表或 processed Parquet 才是 load 后的事实源。

### 股票基础信息表约定

`stock-basic` 是快照型主数据,不依赖交易日历,也不要求传入日期参数。raw 层原样保存 Tushare `stock_basic` 接口字段,包括 `ts_code`, `symbol`, `name`, `area`, `industry`, `fullname`, `enname`, `cnspell`, `market`, `exchange`, `curr_type`, `list_status`, `list_date`, `delist_date`, `is_hs`, `act_name`, `act_ent_type`。

load 阶段只检查目标字段是否存在,然后全量覆盖写入 `dim_security`。`list_status` 保留 Tushare 的 `L/D/P` 语义,分别表示上市、退市、暂停上市;项目不再维护派生字段 `is_active`。

### raw-only 日频数据约定

`stock-st`, `stk-limit`, `suspend-d` 当前只作为原始数据缓存使用。字段表头在 `src/quant/data/fields.py` 中集中声明,fetch 时会作为 Tushare `fields` 参数传入,空结果也会写出固定表头。后续如果这些数据要进入研究层,再单独设计 processed 表和 DuckDB 视图。

### 日频 fetch 的限速与断点续传

拉取 Tushare 日线行情、复权因子、每日指标和 raw-only 日频数据前,需要先完成交易日历加载。日频接口会按 `dim_trade_calendar` 中的开市日逐日请求,并在请求之间固定等待 0.2 秒,避免超过 Tushare 每分钟 500 次的限制。fetch 主流程每拉一天就立即落盘一个 raw CSV,不会把整个时间跨度的数据全部堆积在内存中。

`fetch` 默认跳过已存在的 raw 文件(断点续传),需要强制覆盖时加 `--force`:

```bash
uv run python scripts/run_etl.py fetch daily-ohlcv \
  --source tushare \
  --start-date 20240102 \
  --end-date 20240131
# 中断后重跑,已落盘的 raw CSV 会自动跳过
```

### 日线 processed 层约定

```text
data/processed/ohlcv/year=2024/month=01/ohlcv_202401.parquet   # 月文件
data/processed/ohlcv/year=2024/ohlcv_2024.parquet              # 年文件(归档后)
```

`load daily-ohlcv` 永远先写月文件;如果同一月文件已存在,会读取旧文件和新 raw 合并,并按 `(ts_code, trade_date)` 去重,新数据覆盖旧数据。执行 load 前必须先准备同日期的 `daily-ohlcv`, `stock-st`, `stk-limit`, `suspend-d` raw;缺少任一辅助 raw 会直接失败。推荐顺序:

```bash
uv run python scripts/run_etl.py fetch daily-ohlcv --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py fetch stock-st --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py fetch stk-limit --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py fetch suspend-d --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py load daily-ohlcv --source tushare --start-date 20240102 --end-date 20240131
```

`limit_status` 使用整数编码:`-1=全天停牌`, `0=收盘平盘`, `1=上涨(不含涨停)`, `2=涨停`, `3=下跌(不含跌停)`, `4=跌停`。`is_suspended` 表示全天停牌;`suspend-d` 中 `suspend_type=R` 或 `suspend_timing` 非空的日内临停不会补全天停牌行。`archive daily-ohlcv --year YYYY` 只允许归档已结束年份,会把该年份月文件合并到年文件,写入成功后删除月文件。不要长期同时保留同一年份的月文件和年文件,否则递归读取时会重复统计。

### 复权因子 processed 层约定

```text
data/processed/adj_factor/year=2024/month=01/adj_factor_202401.parquet
```

processed 标准字段固定为 `ts_code`, `trade_date`, `cumulative_factor`。raw 层保留 Tushare 原始字段 `adj_factor`,load 阶段再映射为项目内部标准字段,避免后续 Repository 和回测直接依赖某个数据源的字段含义。

### 每日指标 processed 层约定

```text
data/processed/daily_basic/year=2024/month=01/daily_basic_202401.parquet
```

processed 标准字段对齐 Tushare `daily_basic` 官方文档,包括 `close`, `turnover_rate`, `turnover_rate_f`, `volume_ratio`, `pe`, `pe_ttm`, `pb`, `ps`, `ps_ttm`, `dv_ratio`, `dv_ttm`, `total_share`, `float_share`, `free_share`, `total_mv`, `circ_mv`。其中股本单位为万股,市值单位为万元。

raw 层保留 Tushare 原始返回;load 到 processed 时会做项目语义归一化:`pe` 和 `pe_ttm` 空值表示亏损,入库为 `-1`;`volume_ratio` 空值或负标记表示上市不足五日导致量比为空,入库为 `0`;`dv_ratio` 和 `dv_ttm` 空值或负标记表示未发生派息或股息率为 0,入库为 `0`。`turnover_rate`, `turnover_rate_f`, `total_share`, `free_share`, `float_share`, `total_mv`, `circ_mv` 为空、为 0 或为负数时视为原始数据异常,load 时入库为 `0`,并记录 error 日志用于后续排查。

### 数据状态查询

`status` 直接从目标数据实时聚合,不依赖任何状态表:

- `trade-calendar`:从 `dim_trade_calendar` 表聚合交易所、日期范围、开市天数
- `stock-basic`:从 `dim_security` 表聚合证券总数、交易所数量、上市/退市/暂停上市数量
- `daily-ohlcv`:从 `data/processed/ohlcv/**/*.parquet` 聚合日期范围、行情行数、交易日数、证券数
- `adj-factor`:从 `data/processed/adj_factor/**/*.parquet` 聚合日期范围、因子行数、交易日数、证券数
- `daily-basic`:从 `data/processed/daily_basic/**/*.parquet` 聚合日期范围、指标行数、交易日数、证券数

`etl_manifest` 表仅做兼容保留,当前业务流程不写入也不依赖它。后续如需增量同步基准,再决定是否启用。

## 常用命令

```bash
make install      # uv sync --all-extras
make etl-status   # 查看 daily-ohlcv 状态
make test         # uv run pytest
make test-fast    # uv run pytest -x --no-cov(失败即停,不跑覆盖率)
make lint         # uv run ruff check src/ tests/
make format       # uv run ruff format . && uv run ruff check --fix src/ tests/
make typecheck    # uv run mypy src/
make notebook     # uv run jupyter lab --no-browser
make clean        # 清理缓存目录
```

也可以直接运行:

```bash
uv run pytest
uv run ruff check src/ tests/ scripts/ main.py
uv run mypy src/
```

## 当前状态

已完成:

- 项目目录骨架
- TOML 多层配置加载 + 环境变量机密读取
- Loguru 日志配置(通用日志 + ETL 专用日志)
- A 股日线研究版 DuckDB schema(维表 + Parquet 视图)
- Pydantic v2 数据契约(10 个数据模型,日线 load 末尾做最终校验)
- Repository 查询入口(日线、截面、因子、交易日历、开市日)
- 轻量 ETL 入口(fetch / load / backfill / archive / status)
- Tushare 交易日历 fetch / load / status 完整链路
- Tushare 日线行情 raw fetch(生成器边拉边写 + 断点续传)、月度 load、年度 archive、status
- 基础测试覆盖(87 个测试,覆盖率 94%)

待实现:

- 每日指标、指数日线等更多数据集的 Tushare 适配
- AkShare、MiniQMT 等更多数据源适配
- 日线行情缺失交易日检查和补数辅助命令
- 技术指标和因子 pipeline
- 策略基类和信号生成
- A 股回测撮合(T+1、涨跌停、停牌)、组合、绩效指标

更完整的模块设计见 [lofty-quant-design.md](lofty-quant-design.md)。
