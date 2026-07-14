# features 模块设计文档

本文描述 `src/quant/features/` 的第一版设计。目标不是一次性做完整因子平台,而是补齐一条稳定、可测试、可快速验证的链路:

```text
已注册研究视图
    -> Calculator 原始因子计算
    -> Processor 缺失处理/去极值/标准化
    -> Store processed/factors Parquet
    -> Evaluator 标签/IC/分组收益/换手率
    -> Repository/Notebook/策略读取
```

第一版代码由实现者按本文分步落地。本文只定义边界、契约、模块职责和验收标准,不包含生产代码实现。

## 1. 当前项目上下文

当前仓库已经具备比较清晰的数据层:

- `src/quant/data/db.py`: 初始化 DuckDB 表,并把 `data/processed/*` 注册为视图。
- `src/quant/data/repository.py`: 研究数据唯一公开查询入口。
- `data/processed/ohlcv`: 股票日线行情,注册为 `v_daily_ohlcv`。
- `data/processed/adj_factor`: 累计复权因子,注册为 `v_adj_factor`。
- `data/processed/daily_basic`: 每日估值和股本指标,注册为 `v_daily_basic`。
- `data/processed/factors`: 因子缓存目录,注册为 `v_factors`。

`QuantRepository.get_factors()` 已经约定了因子结果为 long format:

```text
ts_code | trade_date | factor_name | factor_value | factor_version
```

`quant.data.fields.FACTOR_COLUMNS` 也已经定义相同字段。第一版 features 模块应沿用这个契约,不要重新设计宽表或单因子单文件存储。

## 2. 设计目标

第一版 features 模块优先满足四件事:

1. 快速验证: 给定日期区间和因子名,可以本地计算、落盘、查询、做简单质量检查。
2. 可扩展: 新增一个因子时只需要新增一个纯计算函数并注册,不需要改 pipeline 主流程。
3. 可回测: 因子值、处理后因子和未来收益标签都有明确时间语义,避免未来数据。
4. 可测试: 因子公式、写入覆盖、查询契约、处理规则和评估指标都能用固定小数据测试。

暂不追求:

- 多进程/分布式计算。
- 在线调度和任务状态表。
- 因子表达式 DSL。
- 完整因子研究平台。
- 行业中性化、市值中性化、barra 风格风险模型。
- 自动生成策略或回测结果。
- 第一版强制行业/市值中性化。

## 3. 时间语义

第一版统一采用如下时间语义:

- `trade_date = T` 的因子值,只使用 `T` 日收盘后已知的数据。
- `T` 日因子最早只能用于 `T+1` 交易决策。
- 技术因子使用后复权价格或显式 as-of 复权逻辑,不能使用最新口径前复权价格直接回看历史。
- rolling 因子计算器保留 warmup 行并以 null 表达观测不足;后续 Pipeline 再把最终结果裁剪到用户请求区间。
- 因子校验和 RankIC 使用未来收益时,未来收益只能用于分析输出,不能写回因子结果。
- 如果按 `T` 日收盘后出信号、`T+1` 日开盘成交来评估,则 5 日未来收益标签应使用 `hfq_open(T+6) / hfq_open(T+1) - 1`,而不是简单使用 `close(T+5) / close(T) - 1`。

因此第一版推荐使用 `v_daily_hfq` 计算价格收益类因子。如果改用 `v_daily_ohlcv + v_adj_factor` 自行计算复权价格,也必须保持同样的 as-of 约束。

## 4. 模块边界

第一版按四层职责理解,但代码目录保持轻量,不急着拆成大量子包:

```text
Calculator -> Processor -> Store -> Evaluator
```

### Calculator

Calculator 只负责计算原始因子值。输入是历史行情、估值或其他研究面板。首个
`return_5d` 切片直接输出兼容存储层的 long format:

```text
ts_code | trade_date | factor_name | factor_value | factor_version | raw_value
```

raw-only 阶段 `factor_value = raw_value`;这只是当前可消费值的兼容列,不代表
Calculator 执行了截面处理。

Calculator 不负责:

- 选股。
- 分组。
- 仓位分配。
- 回测。
- 落盘。
- 绩效统计。

### Processor

Processor 负责把原始因子转成研究或策略更适合消费的处理后因子,包括:

- 缺失值标记。
- 截面去极值。
- 截面标准化。
- 截面排名。
- 后续可选行业/市值中性化。

第一版 Processor 的核心原则是保留缺失,不要用 0 填充。缺失不等于因子值为零,可能来自上市时间不足、停牌、成交额为 0、输入缺失或回看窗口不足。

### Store

Store 只负责把标准化后的因子结果写入 `data/processed/factors`,并保证重复写入同一唯一键时结果可覆盖。Store 不负责因子计算、处理或评价。

### Evaluator

Evaluator 负责标签和研究评价:

- 未来收益标签。
- 覆盖率。
- IC 和 RankIC。
- 分组收益。
- 多空收益。
- 换手率。
- 因子自相关。

Evaluator 可以读取未来收益做研究评价,但不能把未来收益写入因子缓存,也不能影响 `trade_date = T` 的因子值。

### features 负责

- 从 Repository 或 DuckDB 视图读取研究数据。
- 将输入数据整理为因子计算需要的标准面板。
- 执行因子纯计算函数。
- 执行第一版轻量 Processor。
- 把因子结果标准化为兼容 `FACTOR_COLUMNS` 的 long format。
- 校验因子结果质量。
- 写入 `data/processed/factors`。
- 提供轻量状态、标签和验证能力。

### features 不负责

- 原始数据抓取: 属于 `etl.fetch`。
- raw 到 processed 的数据源清洗: 属于 `etl.load` 和 source normalizer。
- 策略调仓逻辑: 属于 `strategy`。
- 撮合、交易约束、账户权益: 属于 `backtest`。
- 风控和仓位 sizing: 属于 `risk`。
- notebook 中的一次性探索代码长期保存: 应沉淀为 features 或 analysis 模块。

## 5. 目标目录结构

建议分步演进到以下结构:

```text
src/quant/features/
├── DESIGN.md          # 本文档
├── __init__.py
├── base.py            # 因子定义、任务上下文、结果摘要等轻量模型
├── registry.py        # 因子注册表
├── processing.py      # 缺失标记、去极值、标准化、排名
├── storage.py         # 因子结果写入和读取辅助
├── pipeline.py        # 因子计算编排
├── labels.py          # 未来收益标签,只供评估使用
├── validation.py      # 覆盖率、缺失率、IC、RankIC、分组收益等验证
├── technical.py       # 技术类因子
├── fundamental.py     # 基本面/估值类因子
└── alternative.py     # 换手、流动性、资金等另类因子
```

脚本入口建议放在 `scripts/run_factors.py`,而不是 features 目录下。features 只承载可测试的业务能力,脚本只做参数解析和调用。

## 6. 数据契约

### 6.1 输入面板

第一版建议先支持日频股票面板。最小字段:

```text
ts_code
trade_date
close
volume
amount
is_suspended
is_st
limit_status
```

价格类因子建议额外提供:

```text
hfq_open
hfq_high
hfq_low
hfq_close
cumulative_factor
```

每日指标类因子后续再加入:

```text
turnover_rate
turnover_rate_f
volume_ratio
pe
pe_ttm
pb
ps
ps_ttm
total_mv
circ_mv
```

输入面板要求:

- 一行代表一只股票在一个交易日的观测。
- `(ts_code, trade_date)` 在同一输入面板中唯一。
- `trade_date` 使用 `datetime.date` 或可稳定转为 date 的类型。
- 按 `ts_code, trade_date` 排序后再做 rolling 计算。
- 停牌行如果不存在,不强行补齐。第一版以 processed 实际行情行为准。

### 6.2 输出因子表

因子输出必须是 long format:

```text
ts_code
trade_date
factor_name
factor_value
factor_version
```

以上 5 列是第一版必须保留的兼容字段,用于对齐 `quant.data.fields.FACTOR_COLUMNS` 和 `QuantRepository.get_factors()`。

字段约束:

- `ts_code`: 必须为 `000001.SZ` 这类带交易所后缀的代码。
- `trade_date`: 因子所属交易日。
- `factor_name`: 稳定、可读、可查询的因子名,例如 `momentum_20d`。
- `factor_value`: 当前默认可消费因子值。允许缺失,但写入前应统一为 Parquet 可表达的 null/NaN。
- `factor_version`: 因子定义版本,例如 `v1`。

为了支持 raw 和 processed 并存,第一版 Parquet 固定保留 4 个研究列:

```text
raw_value
processed_value
quality_status
created_at
```

这 4 列对调用方是可选输入: 未提供时由存储层补为 null,但输出文件始终保留完整九列。
推荐语义:

- 第一版 raw-only 阶段: `factor_value = raw_value`。
- 启用 Processor 后: `factor_value = processed_value`,同时保留 `raw_value` 供研究追溯。
- `quality_status`: `valid`, `insufficient_history`, `suspended`, `missing_input`, `invalid_value` 等。
- `created_at`: 写入时间,只用于审计和排查,不参与因子计算。

这样可以兼容现有 Repository 查询,同时给后续处理后因子留扩展空间。不要删除 `factor_value`,否则会破坏当前查询契约。

唯一键:

```text
ts_code + trade_date + factor_name + factor_version
```

重复写入同一唯一键时,新数据覆盖旧数据。

### 6.3 因子定义

每个因子至少应声明:

```text
name
version
category
lookback_days
required_fields
higher_is_better
min_periods
description
```

建议约定:

- `name`: 不包含版本,例如 `momentum_20d`。
- `version`: 单独存放,例如 `v1`。
- `category`: `technical`, `fundamental`, `alternative`。
- `lookback_days`: 计算所需最大历史交易日数量。
- `required_fields`: 计算所需的逻辑字段,例如 `hfq_close`,不绑定具体存储视图。
- `higher_is_better`: 因子排序方向。若方向需要由研究决定,可以为 `None`。
- `min_periods`: rolling 计算最少有效观测数。
- `description`: 面向研究者的简短说明。

第一版固定使用日频并保留缺失值,因此不在元数据中重复声明 `frequency` 和
`null_policy`。数据读取位置由 Repository/Pipeline 根据 `required_fields` 决定,不使用单个
`input_view` 绑定存储实现。`output_unit` 和财务数据可用时间模型等到出现实际消费者后再引入。

## 7. 存储设计

因子结果保存到:

```text
data/processed/factors/year=YYYY/month=MM/factors_YYYYMM.parquet
```

已结束年份可以后续再支持归档:

```text
data/processed/factors/year=YYYY/factors_YYYY.parquet
```

第一版只需要支持月文件写入。归档可以等因子计算稳定后再做。

写入规则:

- 公共入口为 `write_factor_results(processed_dir, df)`,其中 `processed_dir` 是
  `data/processed` 根目录。
- 使用现有日频 processed 写入思路,按 `trade_date` 所在年月切分。
- 每次写入前读取同月旧文件,按唯一键去重。
- 新结果覆盖旧结果。
- 空 DataFrame 不写文件;非空输入缺少 `FACTOR_COLUMNS` 或包含未支持字段时明确报错。
- 输出固定包含 `FACTOR_COLUMNS` 和 `raw_value`, `processed_value`, `quality_status`,
  `created_at`;缺失的研究列写为 null。
- 存储层不自动刷新 DuckDB。Pipeline 在写入后负责调用
  `DuckDBManager.refresh_views()` 刷新 `v_factors`。
- 不写 DuckDB 实体表,继续使用 Parquet 作为事实源。
- DuckDB 读取 Hive 路径时自动暴露的 `year`, `month` 分区列属于预期行为。

不建议第一版采用:

- 一个因子一个目录,例如 `factor_name=momentum_20d/factor_version=v1/year=2025/`。
- 一个因子一张 DuckDB 表。
- 宽表 `ts_code, trade_date, factor_a, factor_b` 作为主存储。

原因是当前 Repository 已经按 long format 查询,并且当前 processed 写入工具天然适配按日期切分的月文件。等因子数量和文件体积明显增大后,再评估是否引入 `factor_name` 或 `factor_version` 作为 Hive 分区。

## 8. 第一批因子范围

第一版建议只做依赖 `ohlcv + adj_factor` 的技术因子。这样可以直接使用本地已有历史行情和复权因子数据,不依赖新的数据源。

建议首批因子:

| 因子名 | 类别 | 依赖字段 | 方向 | 说明 |
| --- | --- | --- | --- | --- |
| `return_5d` | technical | `hfq_close` | `False` | 5 日收益。短期反转研究时由评估或策略反向排序,不要把原始值乘以 -1 后落盘 |
| `momentum_20d` | technical | `hfq_close` | `True` 或 `None` | 20 日动量收益 |
| `volatility_20d` | technical/risk | `hfq_close` | `False` | 20 日日对数收益率波动率,第一版不年化 |
| `log_amount_mean_20d` | alternative/liquidity | `amount` | `True` 或 `None` | 20 日平均成交额取对数,比成交量更适合跨股价比较 |
| `amihud_20d` | alternative/liquidity | `hfq_close`, `amount` | `False` | 非流动性度量,`mean(abs(ret) / amount)` |

公式约定:

```text
return_5d = hfq_close(t) / hfq_close(t-5) - 1
momentum_20d = hfq_close(t) / hfq_close(t-20) - 1
daily_log_return = ln(hfq_close(t) / hfq_close(t-1))
volatility_20d = std(daily_log_return, 20)
log_amount_mean_20d = log(mean(amount, 20))
amihud_20d = mean(abs(hfq_close(t) / hfq_close(t-1) - 1) / amount, 20)
```

`amount <= 0` 的观测不应参与 `log_amount_mean_20d` 和 `amihud_20d` 计算,应标记为缺失或 `invalid_value`。若需要缩放 `amihud_20d`,可以在因子版本说明中明确乘数,例如 `x 1e8`。

如果已经具备可靠的历史流通市值,可以作为可选第六个因子:

```text
size_log_float_mcap = log(float_market_cap)
```

但必须使用历史时点流通股本和当日价格,不能用当前流通市值回填历史。若暂时没有可靠历史股本数据,第一版不要实现市值因子。

第一版不建议加入:

- 行业中性化因子。
- 市值中性化因子。
- 财报公告日对齐因子。
- 依赖停牌补行的因子。
- 需要分钟线的因子。

这些都可以后续扩展,但会显著增加第一版边界。

## 9. Pipeline 设计

### 9.1 计算流程

一次因子计算任务建议按以下步骤执行:

1. 解析任务参数: 因子名、日期范围、版本、是否 dry-run。
2. 加载配置并初始化 DuckDB 视图。
3. 从 registry 找到对应因子定义。
4. 根据最大 `lookback_days` 计算 warmup 起始日期。
5. 读取 warmup 起始日至 end date 的输入面板。
6. 按因子定义执行 Calculator,生成 `raw_value`。
7. 根据回看窗口、停牌、零成交额、输入缺失等生成 `quality_status`。
8. 裁剪输出到用户请求的 start date 至 end date。
9. 执行第一版 Processor: 保留缺失、按日截面去极值、按日截面排名或 Z-score。
10. 生成兼容输出字段: `factor_value`, `factor_name`, `factor_version`。
11. 做基础质量校验。
12. 写入 `data/processed/factors`。
13. 刷新 DuckDB 视图。
14. 返回计算摘要。

### 9.2 任务参数

建议最小任务参数:

```text
factor_names
start_date
end_date
factor_version
dry_run
processor
```

后续可扩展:

```text
universe
exclude_st
exclude_suspended
min_history_days
neutralization
```

Step 5 的 `processor` 只支持以下固定枚举:

```text
raw
rank_pct
```

`raw` 是 Step 5 默认值,表示 `factor_value = raw_value`。`rank_pct` 表示按交易日
对有效原始值做平均百分位排名。Z-score、中性化等处理延后实现,避免 Pipeline
提前复杂化。

### 9.3 计算摘要

每次计算应返回可打印摘要:

```text
factor_names
factor_version
start_date
end_date
warmup_start_date
input_row_count
output_row_count
written_paths
missing_value_rate
valid_rate
processor
```

摘要用于 CLI、日志和测试断言。

## 10. Repository 扩展建议

为了遵守项目规则,业务代码不要在 features 各处散落 SQL。建议在 `QuantRepository` 增加一个批量面板读取接口。

建议接口语义:

```text
get_daily_panel(start, end, fields, adjustment="hfq")
```

职责:

- 从 `v_daily_ohlcv`, `v_daily_hfq`, `v_daily_basic` 等视图读取字段。
- 校验字段名,避免 SQL 注入。
- 返回按 `ts_code, trade_date` 排序的记录或 DataFrame。

第一版也可以先在 `pipeline.py` 中集中写一处 SQL,但应视为过渡实现。若这样做,需要在文档或 TODO 中明确后续迁移到 Repository。

## 11. Validation 设计

第一版验证分两层。

### 11.1 写入前基础校验

写入前必须检查:

- 输出字段齐全。
- `factor_name` 不为空。
- `factor_version` 不为空。
- `trade_date` 在请求区间内。
- 唯一键没有重复。
- `factor_value` 可转为数值。
- 如果存在 `raw_value`, `processed_value`, `quality_status`,需要和 `factor_value` 语义一致。
- 非空结果至少覆盖一个交易日。

### 11.2 研究质量验证

`validate` 命令或函数建议输出:

```text
row_count
trade_date_count
security_count
missing_value_rate
coverage_by_date
mean
std
min
p25
median
p75
max
ic_5d_mean
rank_ic_5d_mean
rank_ic_5d_std
rank_ic_5d_count
ic_ir
positive_ic_ratio
q1_return
q2_return
q3_return
q4_return
q5_return
long_short_return
turnover
factor_autocorr_1d
```

### 11.3 未来收益标签

Evaluator 需要生成未来收益标签,但标签只用于评估,不写入因子缓存。

第一版只生成:

```text
forward_return_5d = hfq_open(T+6) / hfq_open(T+1) - 1
```

该定义对应:

```text
T 日收盘后计算因子
T+1 日开盘买入
持有 5 个交易日
T+6 日开盘卖出
```

`T+1` 和 `T+6` 必须由 SSE 交易日历映射,不能按个股已有行情行做
`shift`。个股在目标交易日停牌、缺行或开盘价缺失时,该标签为 null;请求区间
末端未来日历不足时同样保留 null。标签只在内存中参与研究评价,不保存到
Parquet,也不增加到因子表。

如果后续策略改为下一日收盘成交,标签也必须随交易假设同步调整。

### 11.4 IC 和分组收益规则

第一版 IC/RankIC 规则:

- 使用 `T` 日因子值和 `forward_return_5d`。
- 收益使用后复权开盘价构造。
- 每个交易日分别计算截面 Pearson IC 和 Spearman RankIC,并列值使用平均排名。
- 每个有效截面至少需要 5 个因子和收益都有效的股票;样本不足或任一序列无方差时跳过该日期。
- 日度指标先按日期计算,再跨日期等权平均;RankIC 标准差使用样本标准差。
- 只输出汇总,不把未来收益写入因子缓存。

如果某个日期样本太少,跳过该日期并在摘要中报告有效日期数。

分组收益规则:

- 每个交易日按 `factor_value` 或指定 `processed_value` 分为 5 组。
- 分别输出 Q1 到 Q5 的平均未来收益。
- 输出方向多空收益。`higher_is_better=True` 使用 `Q5-Q1`,False 使用
  `Q1-Q5`,方向未知时返回 `None`。
- 检查收益是否大致单调,但第一版不要把单调性作为硬性通过条件。

换手率规则:

```text
turnover_t = 0.5 * sum(abs(weight_t - weight_t-1))
```

第一版使用元数据方向最优的单边组估算换手率:高值优先评价 Q5,低值优先评价
Q1。组内等权,只在相邻有效评价日之间计算;方向未知或有效日期少于两个时返回
`None`,不接入完整回测引擎。

因子自相关规则:

```text
factor_autocorr_1d = corr(factor_value_t, factor_value_t-1)
```

它用于判断因子稳定性和预估组合换手。

覆盖率分母是请求区间内每日 `v_daily_hfq` 的市场行数,分子是同日非空且有限的
`factor_value` 数量。因子缺失率和分布统计只基于已存因子表,且分布忽略 null、
NaN 和无穷值。所有因样本不足、零方差、方向未知或未来行情不足而无法计算的
浮点指标统一返回 `None`,不返回 NaN;`rank_ic_5d_count` 记录实际有效评价日数。

## 12. CLI 设计

建议新增 `scripts/run_factors.py`,提供三个命令。

### 12.1 compute

用途: 计算并写入因子。

参数:

```text
--factors momentum_20d,volatility_20d
--start-date 20240101
--end-date 20240331
--version v1
--processor rank_pct
--dry-run
```

输出:

```text
因子计算完成: factors=2, row_count=..., written_files=...
```

### 12.2 validate

用途: 读取已写入因子并做质量验证。

参数:

```text
--factor momentum_20d
--start-date 20240101
--end-date 20240331
--version v1
```

输出:

```text
覆盖率、缺失率、分布统计、IC/RankIC、五分组收益、换手率摘要
```

### 12.3 status

用途: 查看 `data/processed/factors` 当前状态。

输出:

```text
起始日期
结束日期
因子数量
版本数量
总行数
每个因子的行数
```

## 13. 分步实施计划

### Step 0: 设计冻结

目标:

- 确认本文档中的第一版边界。
- 确认首批因子列表。
- 确认存储契约继续使用 `FACTOR_COLUMNS`。

修改范围:

- 只维护 `src/quant/features/DESIGN.md`。

不做:

- 不写 Python 实现。
- 不改 Repository。
- 不改 CLI。

验收:

- 文档明确第一版做什么、不做什么。
- 每一步可以独立实施和测试。

### Step 1: 因子基础模型和注册表

目标:

- 定义因子描述模型。
- 定义因子注册和查询方式。
- 先注册空的或占位的首批因子元数据。
- 元数据只包含身份、计算依赖和研究方向所需的 8 个字段。

建议修改:

```text
src/quant/features/base.py
src/quant/features/registry.py
src/quant/features/technical.py
tests/test_features/test_registry.py
```

边界:

- 不读取 DuckDB。
- 不写 Parquet。
- 不实现 pipeline。
- 不计算真实因子值。

验收:

- 可以按因子名获取定义。
- 重复注册同名同版本因子会失败。
- 查询不存在的因子会给出清晰错误。
- 首批因子的 `lookback_days` 和 `required_fields` 可被测试断言。
- 首批因子的方向字段可被测试断言,例如 `return_5d.higher_is_better = False`。

### Step 2: 因子存储能力

目标:

- 实现标准因子结果写入 `data/processed/factors`。
- 复用现有日频月度 Parquet 思路。

建议修改:

```text
src/quant/features/storage.py
tests/test_features/test_storage.py
```

边界:

- 输入是已经标准化好的因子 DataFrame。
- 不负责计算因子。
- 不负责读取行情。
- 不负责 RankIC。
- 必须保留 `FACTOR_COLUMNS` 兼容列,可额外保留 `raw_value`, `processed_value`, `quality_status`, `created_at`。

验收:

- 单月因子结果写入 `factors_YYYYMM.parquet`。
- 跨月结果拆分到多个文件。
- 同一唯一键重复写入时新值覆盖旧值。
- 空结果不写文件。
- 缺少 `FACTOR_COLUMNS` 字段时报错。
- 未支持的额外字段时报错,不静默丢弃。
- 额外列存在时不会破坏 `v_factors` 注册和 `get_factors()` 查询。

### Step 3: Repository 面板读取

目标:

- 给 features 提供统一输入数据读取入口。

公开接口:

```text
get_daily_panel(start, end, fields, adjustment="hfq") -> polars.DataFrame
```

- `fields` 只声明计算需要的业务字段,输出自动包含 `ts_code`, `trade_date`。
- 第一版 `adjustment` 只支持 `none`, `hfq`;拒绝使用最新口径 QFQ 构造历史研究面板。
- `none` 读取 `v_daily_ohlcv`,`hfq` 读取 `v_daily_hfq`。

建议修改:

```text
src/quant/data/repository.py
tests/test_data/test_repository.py
```

边界:

- 只新增读取方法。
- 不改已有 `get_daily_bars`, `get_cross_section`, `get_factors` 行为。
- 不引入 features 对 data 层的反向依赖。

验收:

- 可以读取指定日期范围和字段。
- 字段名校验复用或对齐现有 `_validate_fields`。
- `adjustment="hfq"` 时能读取后复权字段。
- 请求当前复权视图不存在的字段时给出明确错误。
- 返回结果按 `ts_code, trade_date` 稳定排序。
- 返回 Polars DataFrame,避免全市场面板转换为 `list[dict]`。

### Step 4: `return_5d` 最小因子闭环

目标:

- 首次只实现 `return_5d`,验证单因子从面板读取到 Repository 回查的完整链路。
- 因子函数保持纯 Polars 计算,不做 IO 和截面处理。

公开计算接口:

```text
compute_return_5d(panel: polars.DataFrame) -> polars.DataFrame
```

输入必须包含 `ts_code`, `trade_date`, `hfq_close`,输出固定为:

```text
ts_code | trade_date | factor_name | factor_value | factor_version | raw_value
```

公式和输出语义:

```text
raw_value = hfq_close(T) / hfq_close(T-5) - 1
factor_value = raw_value
factor_name = return_5d
factor_version = v1
```

计算前按 `ts_code, trade_date` 排序,并按股票独立位移。每只股票前 5 个观测
保留在输出中,`raw_value` 和 `factor_value` 为 null;由后续 Pipeline 负责按请求
区间裁剪 warmup 行。

建议修改:

```text
src/quant/features/technical.py
tests/test_features/test_technical.py
```

边界:

- 不读取配置。
- 计算函数不连接 DuckDB、不写文件。
- 不做中性化。
- 不做未来收益。
- 不生成 `quality_status`,不做去极值、排名或缺失填充。
- 不把短反转直接乘以 -1,通过元数据 `higher_is_better=False` 表达评价方向。
- 不实现 `momentum_20d`, `volatility_20d`, `log_amount_mean_20d` 或 `amihud_20d`。

验收:

- 小样本数据可以精确断言 `return_5d` 公式和前 5 行 null。
- 输入顺序打乱后,输出仍按 `ts_code, trade_date` 正确计算。
- 两只股票交错输入时不跨股票位移。
- 缺少必需字段时错误包含具体字段名。
- 集成测试打通 `get_daily_panel -> compute_return_5d -> to_pandas ->`
  `write_factor_results -> QuantRepository.get_factors`。

### Step 5: 因子计算 Pipeline

目标:

- 先用 `return_5d:v1` 串起 registry、Repository、Calculator、Processor、校验和 storage。
- 提供可直接调用的 `run_factor_pipeline()` 和不可变 `FactorRunSummary`。

建议修改:

```text
src/quant/features/pipeline.py
src/quant/features/processing.py
tests/test_features/test_pipeline.py
```

边界:

- 只支持日频股票因子。
- 只执行已有 Calculator 的 `return_5d:v1`;只有元数据的因子明确报未实现。
- Processor 只支持 `raw` 和 `rank_pct`,默认使用 `raw`。
- 不支持行业/市值中性化。
- 不支持自动调度。
- 不支持并行。
- 不实现 CLI、Z-score 或研究评价。

验收:

- 给定日期范围和因子名,可以生成标准 long format 因子结果。
- warmup 区间被读取,但输出只包含请求区间。
- `dry_run=True` 时不写文件。
- 计算摘要包含输入行数、输出行数、写入路径。
- `processor=raw` 时 `factor_value = raw_value`。
- `processor=rank_pct` 时按交易日截面输出百分位排名。
- 写入后 `QuantRepository.get_factors()` 能查到结果。
- `dry_run=True` 时仍完成计算、处理和校验,但不写入或刷新视图。
- Pipeline 根据交易日历精确读取 warmup,交易日历不足时直接失败。

### Step 6: Validation

目标:

- 提供未来收益标签、基础质量报告、IC/RankIC、五分组收益和换手率摘要。

建议修改:

```text
src/quant/features/labels.py
src/quant/features/validation.py
tests/test_features/test_validation.py
```

边界:

- 只读取已计算因子、SSE 交易日历和后复权开盘价。
- 第一版只实现 `forward_return_5d`,不实现 `forward_return_20d`。
- 固定最少截面样本数为 5。
- 不产生策略信号。
- 不写因子结果。
- 不保存未来收益标签或评价报告文件。
- 不负责画图。
- 标签只用于评估,不写入 `data/processed/factors`。

验收:

- 能输出缺失率、覆盖率、分布统计。
- 能用固定小数据计算 `forward_return_5d`。
- 能用固定小数据计算 RankIC 和五分组收益。
- 能按 `higher_is_better` 估算方向最优单边五分组组合的等权换手率。
- 样本不足时跳过对应日期并报告有效样本数。
- 不可用的浮点指标使用 `None`,不输出 NaN。
- 不把未来收益写入 `data/processed/factors`。

### Step 7: CLI 和 Makefile 入口

目标:

- 提供日常可用命令入口。

建议修改:

```text
scripts/run_factors.py
makefile
README.md
tests/test_features/test_run_factors_cli.py
```

边界:

- CLI 只做参数解析、运行日志和输出摘要。
- 业务逻辑仍在 `src/quant/features/`。
- 不和 ETL 命令混在一起。

验收:

- `compute` 可以 dry-run。
- `compute` 可以实际写入临时测试目录。
- `validate` 可以输出质量摘要。
- `status` 可以读取 factors 当前状态。
- CLI 测试不依赖外部网络。

### Step 8: 扩展每日指标类因子

目标:

- 在技术因子稳定后,引入 `v_daily_basic`。

候选因子:

```text
pb_inverse
pe_ttm_inverse
turnover_rate_20d
float_mv_log
dividend_yield
```

边界:

- 只使用 `daily_basic` 已有字段。
- 不使用财报公告日数据。
- 不做行业中性化。

验收:

- 可以处理 `pe=-1` 这类 processed 层特殊语义。
- 可以区分缺失、亏损、异常值。
- 验证报告能按因子名分别输出覆盖率和分布。

### Step 9: 扩展研究过滤和中性化

目标:

- 在第一版链路稳定后,逐步加入研究常用过滤和变换。

候选能力:

```text
exclude_st
exclude_limit_up_down
min_listing_days
market_cap_neutralize
industry_neutralize
```

边界:

- 基础去极值、标准化、排名属于第一版 Processor;这里主要扩展 universe filter 和中性化。
- 这些应作为独立 transform,不要写死进每个因子函数。
- 中性化后因子建议使用新因子名或新版本,不要覆盖原始因子。

验收:

- 每个 transform 可单独测试。
- 原始因子和处理后因子可同时存在。
- 处理逻辑不改变原始行情和原始因子结果。

## 14. 命名和版本规范

因子名建议:

- 小写 snake_case。
- 包含核心窗口参数,例如 `momentum_20d`。
- 不包含版本号。
- 不包含方向含义模糊的缩写。

版本建议:

- 初始实现为 `v1`。
- 公式变更、输入字段变更、异常值处理变更时升级版本。
- 只改性能、不改结果时不升级版本。

示例:

```text
momentum_20d + v1: 使用 hfq_close 计算 20 日收益。
momentum_20d + v2: 改为跳过涨跌停日或做 winsorize。
return_5d + v1: 使用 hfq_close 计算 5 日收益,方向由 higher_is_better=False 表达。
```

## 15. 日志和错误处理

建议日志绑定:

```text
module="features"
action="compute" | "validate" | "status"
factor_name
factor_version
start_date
end_date
```

错误处理原则:

- 缺少输入视图: 直接失败,提示需要先初始化或加载数据。
- 缺少输入字段: 直接失败,列出缺失字段。
- 因子名不存在: 直接失败,列出可用因子。
- 输出字段不完整: 直接失败。
- 单个因子计算失败: 第一版建议整批失败,不要静默跳过。

不要使用 `except Exception: pass`。

## 16. 测试策略

测试按风险从低到高分层:

1. registry 单元测试: 因子定义、重复注册、缺失查询。
2. technical 单元测试: 固定小数据验证公式。
3. storage 单元测试: 月文件写入、去重覆盖、空数据。
4. repository 测试: 面板读取字段和排序。
5. processing 单元测试: 缺失保留、去极值、排名和 Z-score。
6. pipeline 集成测试: 小型 Parquet 数据完成计算到查询。
7. validation 单元测试: 标签、分布统计、RankIC、分组收益和换手率。
8. CLI 测试: 参数解析和 dry-run。

测试数据要求:

- 使用 `tmp_path`。
- 使用固定小数据。
- 不访问外部网络。
- 不依赖本机真实 `data/processed`。
- 不修改真实 DuckDB 数据库。

建议每一步完成后至少运行:

```text
uv run pytest tests/test_features tests/test_data/test_repository.py
uv run ruff check src/ tests/ scripts/ main.py
uv run mypy src/
```

## 17. 第一版完成标准

第一版完成时,应满足:

- `return_5d`, `momentum_20d`, `volatility_20d`, `log_amount_mean_20d`, `amihud_20d` 可计算。
- 至少支持 `raw` 和 `rank_pct` 两种 Processor。
- 因子结果能写入 `data/processed/factors`。
- `DuckDBManager.initialize()` 能注册 `v_factors`。
- `QuantRepository.get_factors()` 能查到新计算结果。
- 能输出覆盖率、RankIC、五分组收益和换手率摘要。
- 所有新增逻辑有测试。
- 不改变现有 ETL 行为。
- 不引入外部网络依赖。
- 不在 notebook 中保存生产逻辑。

## 18. 后续演进路线

建议演进顺序:

1. 稳定技术因子和存储。
2. 增加 `daily_basic` 估值/流动性因子。
3. 增加 validation 可视化或 report 输出。
4. 增加研究 universe 和过滤规则。
5. 增加标准化、去极值和中性化 transform。
6. 增加策略层消费因子的接口。
7. 增加回测层基于因子截面排序的简单策略模板。
8. 再考虑调度、任务状态表和并行计算。

原则是每次只扩展一层,不要把因子计算、因子评价、策略构建、回测撮合一次性耦合在一起。
