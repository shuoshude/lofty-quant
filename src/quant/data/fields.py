"""数据集字段目录。

本模块只维护字段名、字段顺序和中文说明, 不承载数据源字段映射或清洗规则。
"""

from __future__ import annotations

TUSHARE_DAILY_OHLCV_RAW_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
)
TUSHARE_DAILY_OHLCV_REQUIRED_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
)
TUSHARE_ADJ_FACTOR_RAW_COLUMNS = ("ts_code", "trade_date", "adj_factor")
TUSHARE_ADJ_FACTOR_REQUIRED_COLUMNS = ("ts_code", "trade_date", "adj_factor")
TUSHARE_DAILY_BASIC_RAW_COLUMNS = (
    "ts_code",
    "trade_date",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
)
TUSHARE_DAILY_BASIC_REQUIRED_COLUMNS = ("ts_code", "trade_date")

SECURITY_COLUMNS = (
    "ts_code",
    "symbol",
    "name",
    "exchange",
    "market",
    "list_date",
    "delist_date",
    "is_active",
)
TRADE_CALENDAR_COLUMNS = ("exchange", "cal_date", "is_open", "pretrade_date")
ETL_MANIFEST_COLUMNS = ("dataset", "trade_date", "source", "version", "row_count", "loaded_at")
DAILY_OHLCV_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "volume",
    "amount",
    "is_suspended",
    "is_st",
    "limit_status",
)
ADJ_FACTOR_COLUMNS = ("ts_code", "trade_date", "cumulative_factor")
DAILY_BASIC_COLUMNS = (
    "ts_code",
    "trade_date",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
)
INDEX_DAILY_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "volume",
    "amount",
)
FUNDAMENTAL_COLUMNS = (
    "ts_code",
    "ann_date",
    "report_date",
    "report_type",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "roe",
    "roa",
    "gross_margin",
    "netprofit_yoy",
    "revenue_yoy",
    "debt_to_assets",
    "ocf_to_revenue",
)
FACTOR_COLUMNS = ("ts_code", "trade_date", "factor_name", "factor_value", "factor_version")

RELATION_COMMENTS = {
    "dim_security": "证券主数据表",
    "dim_trade_calendar": "交易所交易日历表",
    "etl_manifest": "兼容保留的旧 ETL 加载状态表",
    "v_daily_ohlcv": "股票日线行情未复权视图",
    "v_adj_factor": "股票每日累计复权因子视图",
    "v_daily_basic": "股票每日估值和股本指标视图",
    "v_index_daily": "指数日线行情视图",
    "v_fundamental": "基本面时点快照视图",
    "v_factors": "量化因子结果视图",
    "v_daily_hfq": "股票日线后复权行情视图",
    "v_daily_qfq_latest": "股票日线最新口径前复权行情视图",
    "v_daily_adj": "兼容旧代码的股票日线复权行情视图",
}

SECURITY_FIELD_COMMENTS = {
    "ts_code": "证券代码, 使用 Tushare 交易所后缀格式",
    "symbol": "证券数字代码",
    "name": "证券简称",
    "exchange": "交易所代码",
    "market": "市场板块",
    "list_date": "上市日期",
    "delist_date": "退市日期",
    "is_active": "是否仍处于活跃交易状态",
}
TRADE_CALENDAR_FIELD_COMMENTS = {
    "exchange": "交易所代码",
    "cal_date": "自然日",
    "is_open": "是否开市",
    "pretrade_date": "上一交易日",
}
ETL_MANIFEST_FIELD_COMMENTS = {
    "dataset": "数据集名称",
    "trade_date": "交易日",
    "source": "数据源名称",
    "version": "数据版本",
    "row_count": "加载行数",
    "loaded_at": "加载完成时间",
}
DAILY_OHLCV_FIELD_COMMENTS = {
    "ts_code": "证券代码, 使用 Tushare 交易所后缀格式",
    "trade_date": "交易日",
    "open": "开盘价",
    "high": "最高价",
    "low": "最低价",
    "close": "收盘价",
    "pre_close": "前收盘价",
    "change": "涨跌额",
    "pct_chg": "涨跌幅, 单位为百分比",
    "volume": "成交量",
    "amount": "成交额",
    "is_suspended": "是否停牌",
    "is_st": "是否 ST 或风险警示",
    "limit_status": "涨跌停状态",
}
ADJ_FACTOR_FIELD_COMMENTS = {
    "ts_code": "证券代码, 使用 Tushare 交易所后缀格式",
    "trade_date": "交易日",
    "cumulative_factor": "累计复权因子",
}
DAILY_BASIC_FIELD_COMMENTS = {
    "ts_code": "证券代码, 使用 Tushare 交易所后缀格式",
    "trade_date": "交易日",
    "close": "当日收盘价",
    "turnover_rate": "换手率",
    "turnover_rate_f": "自由流通股换手率",
    "volume_ratio": "量比",
    "pe": "市盈率",
    "pe_ttm": "滚动市盈率",
    "pb": "市净率",
    "ps": "市销率",
    "ps_ttm": "滚动市销率",
    "dv_ratio": "股息率",
    "dv_ttm": "滚动股息率",
    "total_share": "总股本",
    "float_share": "流通股本",
    "free_share": "自由流通股本",
    "total_mv": "总市值",
    "circ_mv": "流通市值",
}
INDEX_DAILY_FIELD_COMMENTS = {
    **DAILY_OHLCV_FIELD_COMMENTS,
    "pre_close": "前收盘点位",
}
FUNDAMENTAL_FIELD_COMMENTS = {
    "ts_code": "证券代码, 使用 Tushare 交易所后缀格式",
    "ann_date": "公告日期",
    "report_date": "报告期",
    "report_type": "报告类型",
    "pe_ttm": "滚动市盈率",
    "pb": "市净率",
    "ps_ttm": "滚动市销率",
    "roe": "净资产收益率",
    "roa": "总资产收益率",
    "gross_margin": "毛利率",
    "netprofit_yoy": "净利润同比增速",
    "revenue_yoy": "营业收入同比增速",
    "debt_to_assets": "资产负债率",
    "ocf_to_revenue": "经营现金流与收入比",
}
FACTOR_FIELD_COMMENTS = {
    "ts_code": "证券代码, 使用 Tushare 交易所后缀格式",
    "trade_date": "交易日",
    "factor_name": "因子名称",
    "factor_value": "因子值",
    "factor_version": "因子版本",
}
HFQ_FIELD_COMMENTS = {
    **DAILY_OHLCV_FIELD_COMMENTS,
    "cumulative_factor": "累计复权因子",
    "hfq_open": "后复权开盘价",
    "hfq_high": "后复权最高价",
    "hfq_low": "后复权最低价",
    "hfq_close": "后复权收盘价",
}
QFQ_LATEST_FIELD_COMMENTS = {
    **DAILY_OHLCV_FIELD_COMMENTS,
    "cumulative_factor": "当日累计复权因子",
    "latest_cumulative_factor": "最新累计复权因子",
    "qfq_open": "最新口径前复权开盘价",
    "qfq_high": "最新口径前复权最高价",
    "qfq_low": "最新口径前复权最低价",
    "qfq_close": "最新口径前复权收盘价",
}
DAILY_ADJ_FIELD_COMMENTS = {
    **DAILY_OHLCV_FIELD_COMMENTS,
    "cumulative_factor": "累计复权因子",
    "adj_open": "兼容字段, 前复权开盘价",
    "adj_high": "兼容字段, 前复权最高价",
    "adj_low": "兼容字段, 前复权最低价",
    "adj_close": "兼容字段, 前复权收盘价",
}

COLUMN_COMMENTS = {
    "dim_security": SECURITY_FIELD_COMMENTS,
    "dim_trade_calendar": TRADE_CALENDAR_FIELD_COMMENTS,
    "etl_manifest": ETL_MANIFEST_FIELD_COMMENTS,
    "v_daily_ohlcv": DAILY_OHLCV_FIELD_COMMENTS,
    "v_adj_factor": ADJ_FACTOR_FIELD_COMMENTS,
    "v_daily_basic": DAILY_BASIC_FIELD_COMMENTS,
    "v_index_daily": INDEX_DAILY_FIELD_COMMENTS,
    "v_fundamental": FUNDAMENTAL_FIELD_COMMENTS,
    "v_factors": FACTOR_FIELD_COMMENTS,
    "v_daily_hfq": HFQ_FIELD_COMMENTS,
    "v_daily_qfq_latest": QFQ_LATEST_FIELD_COMMENTS,
    "v_daily_adj": DAILY_ADJ_FIELD_COMMENTS,
}
