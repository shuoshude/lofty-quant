# AGENT.md

```markdown
Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.



# lofty-quant 项目特定规则

## 包管理（严格遵守）
- 安装依赖：`uv add <package>`
- 安装开发依赖：`uv add --dev <package>`
- 运行命令：`uv run <command>`
- 禁止使用 pip、pip install、bare python

## 常用命令
- ETL：`uv run python scripts/run_etl.py backfill daily-ohlcv --source tushare --start-date 20240101 --end-date 20240131`
- 回测：`uv run python scripts/run_backtest.py --strategy momentum`
- 测试：`uv run pytest`
- 覆盖率：`uv run pytest --cov=src --cov-report=term-missing`
- 格式化：`uv run ruff format .`
- 检查：`uv run ruff check src/ tests/`
- 类型检查：`uv run mypy src/`

## Python 编码规范
- Python 3.12+，所有函数必须有 type hints 和 docstrings
- 用 pathlib.Path 处理文件路径，绝不用字符串拼接路径
- 大数据集用 polars，小数据集（<10万行）用 pandas
- HTTP 请求用 httpx，禁用 requests
- 数据验证用 pydantic v2
- 所有 IO 操作用 pathlib，禁止 os.path

## 数据层规则（重要）
- `data/raw/` 是原始落盘层，只有 fetch 阶段写入；load 只能读取 raw，不修改 raw
- DuckDB 查询只通过 `src/quant/data/repository.py` 进行，禁止在其他模块直接写 SQL
- Parquet 按 year/month 分区写入，文件名格式：`{ts_code}_{trade_date}.parquet`
- DuckDB 连接通过 `src/quant/data/db.py` 的 context manager 管理，禁止裸连接

## A股特有业务规则（必须遵守）
- T+1 规则：当天买入的股票不能当天卖出
- 涨停：只能买入，不能卖出
- 跌停：只能卖出，不能买入
- 停牌股票：不能交易，需跳过信号
- 交易费用：印花税 0.1%（卖出方向），佣金 0.025% 双向，过户费 0.002%
- ts_code 格式：000001.SZ / 600000.SH，禁止使用纯数字代码

## 禁止事项
- 禁止使用全局变量
- 禁止硬编码股票代码、日期、文件路径
- 禁止静默吞掉异常（except: pass 或 except Exception: pass）
- 禁止在 notebooks 里写生产代码
- 禁止提交带输出的 notebook（nbstripout 会处理）
- 禁止在回测中使用未来数据（look-ahead bias）

## 测试要求
- 所有 ETL 函数必须有单元测试
- broker.py 的涨跌停撮合逻辑必须有专项测试
- 测试用固定的 seed 数据，禁止依赖外部网络

```
