"""ETL raw 加载入口。"""

from __future__ import annotations

from quant.config import QuantConfig
from quant.etl.etl_model import ETLTask


def load_raw_data(config: QuantConfig, task: ETLTask) -> int:
    """按数据源分发 raw CSV 到目标存储的加载流程。"""
    if task.source == "tushare":
        from quant.etl.sources.tushare_source import TushareSource

        return TushareSource(config).load_raw(task)
    raise NotImplementedError(f"暂未实现数据源: source={task.source}")
