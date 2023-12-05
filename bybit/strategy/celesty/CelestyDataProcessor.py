import calendar
import datetime
import numpy as np
import os
import pandas as pd
from pprint import pprint
import requests

from bybit.strategy.BybitDataProcessor import BybitDataProcessor

class CelestyDataProcessor:
    def __init__(self, strategy = None, category = None, interval = None, symbol = None):
        self.asset    = "cryptocurrency"
        self.exchange = "bybit"
        self.strategy = strategy
        self.category = category
        self.interval = interval
        self.symbol   = symbol

        self.start_dt = datetime.datetime(2020, 1, 1, 0, 0, 0)
        # self.end_dt   = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self.end_dt   = datetime.datetime.now()

        self.seconds_to_ms = 1000
        self.start_int     = calendar.timegm(self.start_dt.timetuple()) * self.seconds_to_ms
        self.end_int       = calendar.timegm(self.end_dt.timetuple()) * self.seconds_to_ms

        self.bybitDataProcessor = BybitDataProcessor(self.strategy, self.category, self.interval, self.symbol)

    def _get_backtest_df_for_backtest_system(self):
        base_backtest_df = self.bybitDataProcessor._get_base_backtest_df()
        finished_path    = self.bybitDataProcessor._get_finished_path()

        backtest_df = self.get_formatted_backtest_df(base_backtest_df)

        return backtest_df, finished_path

    def get_formatted_backtest_df(self, base_backtest_df):
        base_backtest_df["backtest_data"] = base_backtest_df["funding_rate"]
        base_backtest_df                  = base_backtest_df[["time", "datetime", "open", "funding_rate", "backtest_data"]]

        backtest_df = self.__clean_data(base_backtest_df)

        return backtest_df

    def __clean_data(self, df):
        df.dropna(inplace = True)  # drop Nan value
        # df = df[df != 0].dropna()  # drop "0" value
        df = df.reset_index(drop = True)  # reset row index

        return df