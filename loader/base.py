'''
Author: Qimin Ma
Date: 2026-02-19 11:22:20
LastEditTime: 2026-03-07 12:11:18
FilePath: /TushareDataloader/loader/base.py
Description: This is the base class for all the loaders.
Loader base classes are TushareConstantLoader, TushareDailyLoader, 
which should be inherited by the derived classes.
The TushareConstantLoader is used to load small amount of data without time dimension, such as stock_basic, which is only about 5000 rows.
The TushareDailyLoader is used to load daily time-series data, such as daily. 
Undoubtedly, it is not recommended to load all the dates through out the history every time.
It will be better to load the data only for the missing dates.
And append the missing dates to the existing data folder.
Copyright (c) 2026 by Qimin Ma, All Rights Reserved.
'''
from abc import ABC, abstractmethod
import logging
import os
from dotenv import load_dotenv, find_dotenv
import tushare as ts
from datetime import datetime
from tqdm import tqdm
import time
import pandas as pd
import re
import duckdb

# Calculate today data: YYYY-MM-DD
today = datetime.now().strftime('%Y-%m-%d')

DAILY_PARQUET_PATTERN = re.compile(r"^(\d{8})\.parquet$")
MAX_TRY = 3   # Maximum number of retries
DEFAULT_DELAYS = [1,2,4] # Default delays in seconds



class TushareAPIError(Exception):
    """Raised when a Tushare API call fails after retries or returns invalid data."""
    pass


class DataValidationError(Exception):
    """Raised when required columns or data are missing or invalid."""
    pass



class Loader(ABC):
    def __init__(self,
                db_name:str, 
                table_name:str, 
                logger:logging.Logger=logging.getLogger(__name__), 
                max_retry:int=MAX_TRY,
                default_delays:list[int] = DEFAULT_DELAYS,
                if_saved: bool = True
                ) -> None:

        """
        Args:
            db_name: Database name (used in path {DATAROOT}/{db_name}/...).
            table_name: Table name (subdir and parquet filenames).
            logger: Logger instance.
            max_retry: Maximum number of retries per API call.
            default_delays: Delay in seconds between retries.
            if_saved: If True, write fetched data to Parquet; if False, fetch only (no write).

        """
        self.logger = logger
        self._load_env()
        self.max_retry = max_retry
        self.default_delays = default_delays
        if len(default_delays) != max_retry:
            self.logger.warning(f"The length of default_delays is not equal to max_retry, will use the first {max_retry} delays")
            self.default_delays = self.default_delays[:max_retry]
        self.if_saved = if_saved
        self.table_name = table_name

        # Connect to tushare API
        self._connect_tushare()

        # Make the data directory
        data_root = os.environ.get("DATAROOT")
        os.makedirs(data_root, exist_ok=True)
        self.data_dir = os.path.abspath(f"{data_root}/{db_name}")
        os.makedirs(self.data_dir, exist_ok=True)


    def _load_env(self) -> None:
        """Load .env so that TUSHARE_TOKEN and DATAROOT are available."""
        try:
            load_dotenv(find_dotenv())
        except Exception as e:
            self.logger.error("Error loading environment variables: %s", e)
            raise TushareAPIError(f"Error loading environment variables: {e}") from e


    def _connect_tushare(self):
        TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN")
        self.logger.info(f"Tushare token loaded")
        try:
            self.pro = ts.pro_api(TUSHARE_TOKEN)
        except Exception as e:
            self.logger.error(f"Error setting Tushare token: {e}")
            raise TushareAPIError(f"Error setting Tushare token: {e}")




class TushareConstantLoader(Loader):
    def __init__(self, logger:logging.Logger, 
                db_name:str,
                table_name:str,
                max_retry:int=MAX_TRY,
                default_delays:list[int] = DEFAULT_DELAYS,
                ) -> None:
        super().__init__(db_name, table_name, logger, max_retry, default_delays)
        os.makedirs(f'{self.data_dir}/{table_name}', exist_ok=True)
        self.run()

    def run(self):
        try:
            df = self._run_func()
            path = f"{self.data_dir}/{self.table_name}/{self.table_name}.parquet"
            if df is not None:
                df.to_parquet(path, index=False)
                self.logger.info("Created table %s from %s", self.table_name, self.table_name)
        except Exception as e:
            self.logger.error("Error running %s: %s", self.table_name, e)
            raise TushareAPIError(f"Error running {self.table_name}: {e}")


    @abstractmethod
    def _run_func(self) -> pd.DataFrame:
        """
        Fetch data for one time and return as a single DataFrame.
        The derived class should implement this method.
        For example:
        ```python
        def _run_func(self) -> pd.DataFrame:
            return self.pro.stock_basic()
        ```
        """
        pass





class TushareDailyLoader(Loader):
    """
    This is the loader for daily time-series data.
    You can set the start and end date to load the data.
    If the data is not found, it will load all the data from start to end.
    If the data exists, it will check if the data has missing dates.
    If there are missing dates, it will load the missing dates, and append it to the existing data.
    If there are no missing dates, it will not load the data again.
    The data will be saved in the following format: {DATAROOT}/{db_name}/{table_name}/{YYYYMMDD}.parquet
    """
    def __init__(self, 
                logger: logging.Logger, 
                db_name:str,
                table_name:str,
                start:str='20150101', 
                end:str=today,
                max_retry:int=MAX_TRY,
                default_delays:list[int] = DEFAULT_DELAYS,
                if_saved: bool = True
                ) -> None:
        super().__init__(db_name, table_name, logger, max_retry, default_delays, if_saved)

        self.start = start
        self.end = end
        os.makedirs(f"{self.data_dir}/{self.table_name}", exist_ok=True)

        self.logger.info("Loading data from %s to %s into %s", self.start, self.end, self.data_dir)
        self._date_range()
        self.missing_dates = self._check_missing_dates()
        
        if self.missing_dates:
            self.logger.info("Start running missing dates...")
            self.run()

    
    def _date_range(self):
        """
        Generate the date range from start to end.
        The date range is a list of YYYYMMDD strings.
        """
        start_ymd = self.start.replace("-", "")[:8]
        end_ymd = self.end.replace("-", "")[:8]
        df_cal = self.pro.trade_cal(
            start_date=start_ymd,
            end_date=end_ymd,
            fields="cal_date",
            exchange="SSE",
            is_open=1,
        ).sort_values(by="cal_date")
        self.date_range = df_cal["cal_date"].astype(str).str.replace("-", "").str[:8].values


    def _get_existing_dates(self):
        """
        Get the existing dates from the folder.
        The existing dates are a set of YYYYMMDD strings.
        """
        dir_required = f"{self.data_dir}/{self.table_name}"
        if not os.path.isdir(dir_required): 
            return set()
        existing = set()
        for name in os.listdir(dir_required):
            m = DAILY_PARQUET_PATTERN.match(name)
            if m:
                existing.add(m.group(1))
        return existing


    def _check_missing_dates(self):
        """
        Check if there are missing dates between the date range and the existing dates.
        If there are missing dates, return the missing dates in the order of the date range.
        If there are no missing dates, return None.
        """
        self.logger.info("Checking missing dates: date_range from calendar, existing from folder.")
        existing_dates = self._get_existing_dates()
        date_range_set = set(self.date_range)
        missing = sorted(date_range_set - existing_dates)
        if missing:
            self.logger.info(
                "There are %d missing dates in %s (existing %d in folder).",
                len(missing), self.table_name, len(existing_dates),
            )
            return missing
        self.logger.info("No missing data, no need to load.")
        return None

    def _run_single_date(self, date: str) -> pd.DataFrame:
        """
        Run loader for one date; returns DataFrame from _run_func.
        """
        self.logger.info(f"Running {self.table_name} for date: {date}")
        for i in range(self.max_retry):
            try:
                return self._run_func(date)
            except Exception as e:
                self.logger.error(f"Error running {self.table_name} for date: {date}, error: {e}")
                if i < self.max_retry - 1:
                    self.logger.info(f"Retry {i+1} of {self.max_retry} for {self.table_name} for date: {date}")
                    time.sleep(self.default_delays[i])
                else:
                    raise TushareAPIError(f"Error running {self.table_name} for date: {date}, error: {e}")

    def _daily_file_path(self, date: str) -> str:
        """
        Get the file path for the daily data.
        The file path is {DATAROOT}/{db_name}/{table_name}/{YYYYMMDD}.parquet
        """
        return f"{self.data_dir}/{self.table_name}/{date}.parquet"


    def run(self):
        """
        Run loader for all missing dates.
        """
        for date in tqdm(self.missing_dates, desc=f"Running {self.table_name}"):
            try:
                df = self._run_single_date(date)
                if self.if_saved:
                    if df is not None and not df.empty:
                        path = self._daily_file_path(date)
                        df.to_parquet(path, index=False)
                        self.logger.info("Wrote %s -> %s (%d rows)", date, path, len(df))
                    else:
                        return df
            except Exception as e:
                self.logger.error("Error running %s for date %s: %s", self.table_name, date, e)
                continue


    @abstractmethod
    def _run_func(self, date: str) -> pd.DataFrame:
        """
        Fetch data for one date and return as a single DataFrame.
        The derived class should implement this method.
        For example:
        ```python
        def _run_func(self, date: str) -> pd.DataFrame:
            return self.pro.daily(trade_date=date)
        ```
        """
        pass



