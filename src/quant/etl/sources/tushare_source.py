"""Tushare 数据源适配器。"""

from __future__ import annotations

from typing import Any, cast

import tushare as ts
from pandas import DataFrame

from quant.config import QuantConfig
from quant.etl.etl_model import ETLTask


class TushareClient:
    """Tushare API 管理。"""

    def __init__(self, config: QuantConfig) -> None:
        self._tushare_token = config.secrets.tushare_token
        if not self._tushare_token:
            raise ValueError("请在环境变量中设置 LOFTY_QUANT__SECRETS__TUSHARE_TOKEN")
        ts.set_token(self._tushare_token)
        self._pro_api: Any = ts.pro_api()

    def fetch_tushare_raw(self, task: ETLTask) -> DataFrame:
        """按数据集拉取 Tushare 原始 DataFrame。"""
        if task.dataset == "trade-calendar":
            return self.fetch_trade_calendar(task)
        raise NotImplementedError(f"暂未实现数据集: dataset={task.dataset}, source={task.source}")

    def fetch_trade_calendar(self, task: ETLTask) -> DataFrame:
        """拉取交易日历原始数据。"""
        exchange = task.exchange or ""
        result = self._pro_api.trade_cal(
            exchange=exchange,
            start_date=task.start_date.strftime("%Y%m%d"),
            end_date=task.end_date.strftime("%Y%m%d"),
        )
        return cast(DataFrame, result)
