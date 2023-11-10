import datetime
import numpy as np
import os
import pandas as pd
from pprint import pprint
import requests
import shutil
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 320)

class BacktestResultManager:
    def __init__(self):
        self.asset      = "Cryptocurrency"
        self.strategy   = "testing_basis_index_price_open_interest"
        self.exchange   = "binance"
        self.instrument = "futures"
        self.product    = "coinm_futures"
        self.type       = "PERPETUAL"
        self.price_func = "kline"
        self.interval   = "8h"

    def _delete_useless_backtest_result(self):
        full_symbol_result_df = self._get_full_symbol_result_df()
        delete_list           = self._get_delete_list(full_symbol_result_df)
        self._delete_folders(delete_list)

    def get_full_symbol_result_path(self):
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = self.price_func
        interval   = self.interval

        full_symbol_result_path = f"D:/backtest/{asset}/{strategy}/{exchange}/{instrument}/{product}/{interval}"

        return full_symbol_result_path

    def _get_full_symbol_result_df(self):
        full_symbol_result_path = self.get_full_symbol_result_path()
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
        result_path = self.get_full_symbol_result_path()

        for symbol in delete_list:
            symbol_path = os.path.join(result_path, symbol)
            shutil.rmtree(symbol_path)

def main():
    backtestResultManager = BacktestResultManager()
    backtestResultManager._delete_useless_backtest_result()

if __name__ == "__main__":
    main()