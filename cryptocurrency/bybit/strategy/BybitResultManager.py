import datetime
import numpy as np
import os
import pandas as pd
from pprint import pprint
import requests
import shutil
import time

class BybitResultManager:
    def __init__(self, strategy, category, interval, delete_file):
        self.asset      = "cryptocurrency"
        self.exchange   = "bybit"
        self.strategy   = strategy
        self.category   = category
        self.interval   = interval
        self.price_func = "kline"

        self.set = "backtest_set"

        self.delete_file = delete_file

    def _delete_useless_backtest_result(self):
        full_symbol_result_df = self._get_full_symbol_result_df()
        delete_list           = self._get_delete_list(full_symbol_result_df)

        if self.delete_file == True:
            self._delete_folders(delete_list)
            print(f"{self.exchange}丨{self.strategy}丨{self.category}丨{self.interval}丨action = deleted useless backtest file")

        else:
            print(f"{self.exchange}丨{self.strategy}丨{self.category}丨{self.interval}丨action = generated useless backtest file_list")

    def _get_full_symbol_result_path(self):
        full_symbol_result_path = f"D:/backtest/{self.asset}/{self.exchange}/{self.strategy}/{self.category}/{self.interval}"

        return full_symbol_result_path

    def _get_full_symbol_result_df(self):
        full_symbol_result_path = self._get_full_symbol_result_path()
        full_symbol_result_csv  = f"{full_symbol_result_path}/full_symbol_backtest_result.csv"

        full_symbol_result_df = pd.read_csv(full_symbol_result_csv)

        return full_symbol_result_df

    def _get_delete_list(self, full_symbol_result_df):
        result_df   = full_symbol_result_df
        delete_list = []

        for i in range(len(result_df)):
            symbol        = result_df.loc[i, "symbol"]
            rubbish_strat = result_df.loc[i, "rubbish_strat"]

            if rubbish_strat == True:
                delete_list.append(symbol)

        return delete_list

    def _delete_folders(self, delete_list):
        result_path = self._get_full_symbol_result_path()

        for symbol in delete_list:
            symbol_path = os.path.join(result_path, symbol)

            try:
                shutil.rmtree(symbol_path)
                print(f"{self.exchange}丨{self.strategy}丨{self.category}丨{self.interval}丨{symbol}丨action = deleted backtest file")

            except FileNotFoundError:
                pass