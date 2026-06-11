# lofty-quant

个人 A 股量化交易系统。当前项目处于基础设施搭建阶段，已经完成项目骨架、TOML 配置加载和 Loguru 日志配置；数据 ETL、因子、策略、回测等模块目前仍是占位目录，后续按设计文档逐步实现。

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
│   ├── data/                         # 数据访问层占位
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
- 基础测试覆盖

待实现：

- A 股数据 schema
- DuckDB 连接和 repository
- ETL 读取、清洗、转换、加载
- 技术指标和因子 pipeline
- 策略基类和信号生成
- A 股回测撮合、组合、绩效指标

更完整的模块设计见 [lofty-quant-design.md](lofty-quant-design.md)。
