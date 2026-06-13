import tushare as ts
from tushare.pro.client import DataApi
from pandas import DataFrame
from quant.config import load_config, QuantConfig
from pathlib import Path
from ..etl_model import ETLTask


class TushareClient:
    '''tushare api管理'''
    def __init__(self) -> None:
        self._tushare_token = load_config().secrets.tushare_token
        if not self._tushare_token:
            raise ValueError("请在环境变量中设置 LOFTY_QUANT__SECRETS__TUSHARE_TOKEN")
        ts.set_token(self._tushare_token)
        self._pro_api = ts.pro_api()


    def fetch_tushare_raw(self, raw_dir: Path, task: ETLTask)-> DataFrame:
        if task.dataset == "trade-calendar":
            return self.fetch_trade_calendar(raw_dir, task)
        else :
            raise NotImplementedError(...)


    def fetch_trade_calendar(self, raw_dir: Path, task: ETLTask)-> DataFrame:
        df: DataFrame = self._pro_api.trade_cal(exchange='',start_date=task.start_date.strftime('%Y%m%d'), end_date=task.end_date.strftime('%Y%m%d'))
        return df


    def load_trade_calendar(self, config: QuantConfig, task: ETLTask) -> int:
        return 1