# lofty-quant

个人 A 股量化交易系统。已完成项目骨架、多层 TOML 配置、Loguru 日志,以及 `Tushare -> raw CSV -> DuckDB/Parquet` 的交易日历、证券主数据、日线行情、复权因子、每日指标和 raw-only 辅助数据轻量 ETL 链路;因子、策略、回测等量化模块按实际使用逐步实现。

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
│   ├── etl/                          # 轻量 ETL(fetch/load/daily/processed/storage)
│   │   ├── processed.py              # 日频 processed Parquet 写入和归档
│   │   └── sources/                  # Tushare 数据源适配器和字段标准化
│   ├── features/                     # 因子工程(占位)
│   ├── strategy/                     # 策略层(占位)
│   ├── backtest/                     # 回测引擎(占位)
│   ├── risk/                         # 风控模块(占位)
│   └── analysis/                     # 分析输出(占位)
├── tests/                            # 单元测试(191 个,覆盖率 92%)
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

- `src/quant/data/schemas.py`:Pydantic 数据契约。`DailyOHLCVRecord` 保留停牌行兼容校验,当前 load 只写 Tushare `daily` 实际返回的交易行情;`DailyBasicRecord` 校验 Tushare `daily_basic` 实际返回并标准化后的每日指标。
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

如果 `data/processed/` 下存在对应 Parquet 文件,还会自动注册视图。当前实际支持的日频研究视图包括 `v_daily_ohlcv`, `v_adj_factor`, `v_daily_basic`:

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

归档成功后会删除对应月文件和空月份目录,避免 DuckDB 视图重复读取同一批日频数据。复权因子、每日指标等日频数据也先写月文件,已结束年份可通过 `archive` 合并为年文件:

```text
data/processed/adj_factor/year=2024/month=01/adj_factor_202401.parquet
data/processed/daily_basic/year=2024/month=01/daily_basic_202401.parquet
data/processed/adj_factor/year=2024/adj_factor_2024.parquet
data/processed/daily_basic/year=2024/daily_basic_2024.parquet
```

指数行情、因子和财务数据目前保留视图约定,具体 ETL 后续按实际使用补齐。财务数据按公告日期 `ann_date` 分区,避免回测时误用未来数据:

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

ETL 使用 `scripts/run_etl.py` 作为轻量入口。不做 pipeline 框架和自动调度,核心仍是 raw 落盘和 raw 加载两个阶段;盘后单日任务可使用 `daily` 命令按固定顺序编排。

ETL 分为两个阶段:

```text
外部数据源 -> fetch -> data/raw -> load -> DuckDB 或 processed Parquet
```

阶段命令可以独立执行。ETL 使用 Tushare `SSE` 交易日历作为本地统一 A 股开闭市日历,脚本入口不再暴露交易所参数。

推荐按依赖顺序准备数据:

```bash
uv run python scripts/run_etl.py fetch trade-calendar --source tushare --start-date 20130101 --end-date 20260630
uv run python scripts/run_etl.py load trade-calendar --source tushare --start-date 20130101 --end-date 20260630

uv run python scripts/run_etl.py fetch stock-basic --source tushare
uv run python scripts/run_etl.py load stock-basic --source tushare

uv run python scripts/run_etl.py fetch daily-ohlcv --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py fetch stock-st --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py fetch stk-limit --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py fetch suspend-d --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py load daily-ohlcv --source tushare --start-date 20240102 --end-date 20240131

uv run python scripts/run_etl.py fetch adj-factor --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py load adj-factor --source tushare --start-date 20240102 --end-date 20240131

uv run python scripts/run_etl.py fetch daily-basic --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py load daily-basic --source tushare --start-date 20240102 --end-date 20240131
```

生命周期约定:

- `fetch`:只连接外部数据源,只写 `data/raw`,不写 DuckDB,不写 processed。
- `load`:只读取 `data/raw`,清洗转换后写 DuckDB 或 processed。
- `backfill`:历史回填,按 `fetch -> load` 执行。除 `stock-basic` 这类快照型数据集外,通常需要显式传入日期范围。
- `archive`:将已结束年份的日频月文件合并为年文件,并删除对应月文件和空月份目录。
- `status`:直接从目标表或 processed 数据实时聚合当前真实状态。
- `missing`:按日期粒度检查指定范围内缺失的数据日期,不自动补数。

### 已支持的数据集

| dataset | fetch | load | archive | status | missing | raw | processed / target |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `trade-calendar` | 支持 | 支持 | 不适用 | 支持 | 支持 | 单文件 CSV | DuckDB `dim_trade_calendar` |
| `stock-basic` | 支持 | 支持 | 不适用 | 支持 | 不适用 | 单文件 CSV | DuckDB `dim_security` |
| `daily-ohlcv` | 支持 | 支持 | 支持 | 支持 | 支持 | 每交易日 CSV | `data/processed/ohlcv` |
| `adj-factor` | 支持 | 支持 | 支持 | 支持 | 支持 | 每交易日 CSV | `data/processed/adj_factor` |
| `daily-basic` | 支持 | 支持 | 支持 | 支持 | 支持 | 每交易日 CSV | `data/processed/daily_basic` |
| `stock-st` | 支持 | 不支持 | 不支持 | 不支持 | 支持 | 每交易日 CSV | raw-only |
| `stk-limit` | 支持 | 不支持 | 不支持 | 不支持 | 支持 | 每交易日 CSV | raw-only |
| `suspend-d` | 支持 | 不支持 | 不支持 | 不支持 | 支持 | 每交易日 CSV | raw-only |

常用状态和归档命令:

```bash
uv run python scripts/run_etl.py daily --source tushare
uv run python scripts/run_etl.py daily --source tushare --date 20260625

uv run python scripts/run_etl.py status trade-calendar --source tushare
uv run python scripts/run_etl.py status stock-basic --source tushare
uv run python scripts/run_etl.py status daily-ohlcv --source tushare
uv run python scripts/run_etl.py status adj-factor --source tushare
uv run python scripts/run_etl.py status daily-basic --source tushare

uv run python scripts/run_etl.py missing daily-ohlcv --source tushare --start-date 20260601 --end-date 20260630
uv run python scripts/run_etl.py missing stock-st --source tushare --start-date 20260601 --end-date 20260630

uv run python scripts/run_etl.py archive daily-ohlcv --source tushare --year 2023
uv run python scripts/run_etl.py archive adj-factor --source tushare --year 2023
uv run python scripts/run_etl.py archive daily-basic --source tushare --year 2023
```

### 每日数据管线

`daily` 是盘后单日入口。默认使用当天日期,也可以通过 `--date YYYYMMDD` 指定某一天;它不会循环多日,也不写 pipeline 状态表。失败后的排查和重跑依赖日志、raw 断点续传和 load 去重覆盖。

每日管线严格按固定顺序执行:

```text
fetch trade-calendar -> load trade-calendar
fetch stock-basic    -> load stock-basic

# 当日开市时继续执行
fetch daily-ohlcv -> fetch stock-st -> fetch stk-limit -> fetch suspend-d -> load daily-ohlcv
fetch adj-factor  -> load adj-factor
fetch daily-basic -> load daily-basic
missing daily-ohlcv / adj-factor / daily-basic / stock-st / stk-limit / suspend-d
```

`load trade-calendar` 后会查询当日是否开市。如果当日休市,管线仍会完成交易日历和股票基础信息更新,然后跳过所有日频数据并视为成功。任一步失败都会立即停止后续步骤,并写入 ETL error 日志;最后的 `missing` 检查失败也会让整条管线失败。`--force` 和 `--dry-run` 会传给 fetch/load 阶段,`missing` 始终实际查询本地数据状态。

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

raw 文件是 fetch 阶段的输入缓存,不代表当前完整数据状态;DuckDB 表或 processed Parquet 才是 load 后的事实源。raw 层保留 Tushare 原始返回,包括历史 `.BJ`、B 股等接口数据,不在 fetch 阶段改写或删除。

### 研究层股票范围过滤

日频 processed 研究层会在 load 阶段统一过滤暂不纳入研究范围的股票:

- `900*.SH` 和 `200*.SZ` B 股数据全部过滤。
- `.BJ` 股票在北交所开市日 `2021-11-15` 之前过滤。
- `.BJ` 股票在 `2021-11-15` 当天及之后保留。

该规则只作用于 `daily-ohlcv`, `adj-factor`, `daily-basic` 的 processed 写入,以及 `stock-st`, `stk-limit` 辅助 raw 参与 OHLCV 状态计算时;不会修改 raw CSV。`suspend-d` 当前保持 raw-only,不参与基础 processed 表补行。

### 股票基础信息表约定

`stock-basic` 是快照型主数据,不依赖交易日历,也不要求传入日期参数。raw 层原样保存 Tushare `stock_basic` 接口字段,包括 `ts_code`, `symbol`, `name`, `area`, `industry`, `fullname`, `enname`, `cnspell`, `market`, `exchange`, `curr_type`, `list_status`, `list_date`, `delist_date`, `is_hs`, `act_name`, `act_ent_type`。

load 阶段只检查目标字段是否存在,然后全量覆盖写入 `dim_security`。`list_status` 保留 Tushare 的 `L/D/P` 语义,分别表示上市、退市、暂停上市;项目不再维护派生字段 `is_active`。

### raw-only 日频数据约定

`stock-st`, `stk-limit`, `suspend-d` 当前只作为原始数据缓存使用。字段表头在 `src/quant/data/fields.py` 中集中声明,fetch 时会作为 Tushare `fields` 参数传入,空结果也会写出固定表头。其中 `stock-st` 和 `stk-limit` 会在 OHLCV load 时用于给已有行情行标注状态;`suspend-d` 是停牌事实 raw 来源,供后续查询、因子或回测层按需使用,不写入基础 processed 表。后续如果这些数据要进入研究层,再单独设计 processed 表和 DuckDB 视图。

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

`load daily-ohlcv` 永远先写月文件;如果同一月文件已存在,会读取旧文件和新 raw 合并,并按 `(ts_code, trade_date)` 去重,新数据覆盖旧数据。执行 load 前必须先准备同日期的 `daily-ohlcv`, `stock-st`, `stk-limit` raw;缺少任一辅助 raw 会直接失败。load 时会统一应用研究层股票范围过滤。processed 中只保存 Tushare `daily` 实际返回的交易行情,不会根据 `suspend-d` 补全天停牌行。

```bash
uv run python scripts/run_etl.py fetch daily-ohlcv --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py fetch stock-st --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py fetch stk-limit --source tushare --start-date 20240102 --end-date 20240131
uv run python scripts/run_etl.py load daily-ohlcv --source tushare --start-date 20240102 --end-date 20240131
```

状态字段来源:

- `stock-st`:生成 `is_st`
- `stk-limit`:结合开收盘价计算 `limit_status`

`is_suspended` 对 Tushare `daily` 实际返回行固定为 `False`;如果需要判断全天停牌事实,后续查询、因子或回测层应读取 `suspend-d` raw。`limit_status` 使用整数编码:`-1=全天停牌(兼容旧数据)`, `0=收盘平盘`, `1=上涨(不含涨停)`, `2=涨停`, `3=下跌(不含跌停)`, `4=跌停`。当前 load 不会生成 `limit_status=-1` 的新增停牌行。`stock-st`, `stk-limit` 辅助 raw 会先过滤历史 `.BJ` 和 B 股,再用于 ST 和涨跌停状态计算。`archive daily-ohlcv --year YYYY` 只允许归档已结束年份,会把该年份月文件合并到年文件,写入成功后删除月文件和空月份目录。不要长期同时保留同一年份的月文件和年文件,否则递归读取时会重复统计。

### 复权因子 processed 层约定

```text
data/processed/adj_factor/year=2024/month=01/adj_factor_202401.parquet
```

processed 标准字段固定为 `ts_code`, `trade_date`, `cumulative_factor`。raw 层保留 Tushare 原始字段 `adj_factor`,load 阶段再映射为项目内部标准字段,避免后续 Repository 和回测直接依赖某个数据源的字段含义。

复权因子按月写入 processed Parquet,load 时会统一应用研究层股票范围过滤。已结束年份可通过 `archive adj-factor --year YYYY` 合并为年度文件。Tushare `adj_factor` raw 已包含全天停牌股票,load 阶段不做停牌补行。

### 每日指标 processed 层约定

```text
data/processed/daily_basic/year=2024/month=01/daily_basic_202401.parquet
```

processed 标准字段对齐 Tushare `daily_basic` 官方文档,包括 `close`, `turnover_rate`, `turnover_rate_f`, `volume_ratio`, `pe`, `pe_ttm`, `pb`, `ps`, `ps_ttm`, `dv_ratio`, `dv_ttm`, `total_share`, `float_share`, `free_share`, `total_mv`, `circ_mv`。其中股本单位为万股,市值单位为万元。

raw 层保留 Tushare 原始返回;load 到 processed 时会做项目语义归一化:`pe` 和 `pe_ttm` 空值表示亏损,入库为 `-1`;`volume_ratio` 空值或负标记表示上市不足五日导致量比为空,入库为 `0`;`dv_ratio` 和 `dv_ttm` 空值或负标记表示未发生派息或股息率为 0,入库为 `0`。`turnover_rate`, `turnover_rate_f`, `total_share`, `free_share`, `float_share`, `total_mv`, `circ_mv` 为空、为 0 或为负数时视为原始数据异常,load 时入库为 `0`,并记录 error 日志用于后续排查。

`daily-basic` load 时会统一应用研究层股票范围过滤,processed 中只保存 Tushare `daily_basic` 实际返回的每日指标,不会根据 `suspend-d` 补全天停牌行。已结束年份可通过 `archive daily-basic --year YYYY` 归档为年度文件。

### 数据状态查询

`status` 直接从目标数据实时聚合,不依赖任何状态表:

- `trade-calendar`:从 `dim_trade_calendar` 表聚合交易所、日期范围、开市天数
- `stock-basic`:从 `dim_security` 表聚合证券总数、交易所数量、上市/退市/暂停上市数量
- `daily-ohlcv`:从 `data/processed/ohlcv/**/*.parquet` 聚合日期范围、行情行数、交易日数、证券数
- `adj-factor`:从 `data/processed/adj_factor/**/*.parquet` 聚合日期范围、因子行数、交易日数、证券数
- `daily-basic`:从 `data/processed/daily_basic/**/*.parquet` 聚合日期范围、指标行数、交易日数、证券数

`etl_manifest` 表仅做兼容保留,当前业务流程不写入也不依赖它。后续如需增量同步基准,再决定是否启用。

### 缺失日期检查

`missing` 只做日期级缺失检查,用于手动补数前快速确认还缺哪些交易日:

```bash
uv run python scripts/run_etl.py missing daily-ohlcv --source tushare --start-date 20260601 --end-date 20260630
uv run python scripts/run_etl.py missing stock-st --source tushare --start-date 20260601 --end-date 20260630
```

检查规则:

- `trade-calendar`:按自然日检查 `dim_trade_calendar` 是否覆盖指定范围。
- `daily-ohlcv`, `adj-factor`, `daily-basic`:以 `dim_trade_calendar` 的开市日为基准,对比对应 processed 视图中的 `trade_date`。
- `stock-st`, `stk-limit`, `suspend-d`:以开市日为基准,只检查对应 raw 日文件是否存在;空 CSV 也算已获取。
- `stock-basic`:快照型数据集,不支持交易日缺失检查。

该命令不检查逐股票缺失、不检查字段质量、不自动补数,也不依赖 `etl_manifest`。

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
- 轻量 ETL 入口(fetch / load / backfill / archive / status / missing / daily)
- Tushare 交易日历 fetch / load / status 完整链路
- Tushare 股票基础信息 fetch / load / status 完整链路
- Tushare 日线行情 fetch / load / archive / status,支持 ST 和涨跌停状态字段
- Tushare 复权因子 fetch / load / archive / status
- Tushare 每日指标 fetch / load / archive / status
- Tushare `stock-st`, `stk-limit`, `suspend-d` raw-only 日频辅助数据 fetch
- 日期级缺失检查命令 `missing`
- 盘后单日数据管线命令 `daily`
- 基础测试覆盖(189 个测试,覆盖率 92%)

待实现:

- 指数日线等更多数据集的 Tushare 适配
- AkShare、MiniQMT 等更多数据源适配
- 逐股票缺失检查和补数辅助命令
- 技术指标和因子 pipeline
- 策略基类和信号生成
- A 股回测撮合(T+1、涨跌停、停牌)、组合、绩效指标

更完整的模块设计见 [lofty-quant-design.md](lofty-quant-design.md)。
