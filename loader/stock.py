'''
Author: Qimin Ma
Date: 2026-03-07 12:03:15
LastEditTime: 2026-03-07 12:03:17
FilePath: /TushareDataloader/loader/stock.py
Description: This is an example of how to use the TushareDailyLoader and TushareConstantLoader classes.
Copyright (c) 2026 by Qimin Ma, All Rights Reserved.
'''

from .base import TushareDailyLoader, TushareConstantLoader
import logging
from datetime import datetime
import pandas as pd

class StockBasic(TushareConstantLoader):
    def __init__(self, logger: logging.Logger) -> None:
        super().__init__(logger, db_name='tushare', table_name='stock_basic')

    def _run_func(self) -> pd.DataFrame:
        return self.pro.stock_basic()


class StockDaily(TushareDailyLoader):
    def __init__(self, logger: logging.Logger, start:str='2015-01-01', end:str=datetime.now().strftime('%Y-%m-%d')) -> None:
        super().__init__(logger, db_name='tushare', table_name='stock_daily', start=start, end=end)

    def _run_func(self, date: str) -> pd.DataFrame:
        return self.pro.daily(trade_date=date)