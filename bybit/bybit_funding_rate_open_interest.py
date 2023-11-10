"""
rmb to change both the initialization
single_result -> inside multiprocessing
full_result   -> outside multiprocessing

Issue
2. Divide the backtest_df into 3 df -> 1) single_training_df, 2) single_testing_df, 3) single_backtest_df
3) Synchornize start_int and end_int in above cases

"""

import calendar
import datetime
import itertools
import math
import matplotlib.pyplot as plt
import multiprocessing as mp
import numpy as np
import os
import pandas as pd
from pprint import pprint
import requests
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 320)

class BacktestSystem():
    def __init__(self, single_backtest_df, finished_path, symbol):
        split_index             = int(0.8 * len(single_backtest_df))
        self.single_backtest_df = single_backtest_df
        self.single_training_df = single_backtest_df.iloc[:split_index]
        self.single_testing_df  = single_backtest_df.iloc[split_index:]

        self.finished_path = finished_path

        self.asset    = "Cryptocurrency"
        self.strategy = "bybit_funding_rate_open_interest"
        self.exchange = "bybit"
        self.category = "linear"
        self.interval = "240"
        self.symbol   = symbol

        self.binance_tx_fee_rate = 0.0002
        self.ann_multiple        = 365 * 3

        self.processes = 1

    def run_backtest(self):
        single_backtest_df   = self.single_backtest_df
        manager_list         = self._get_manager_list()
        para_dict            = self._get_para_dict()
        para_list            = self._get_para_list(para_dict)
        all_para_combination = self._get_all_para_combination(para_list, single_backtest_df, manager_list)
        result_dict          = self._get_result_dict(para_dict)
        para_dict_key_list   = self._get_para_dict_key_list(result_dict)

        pool        = mp.Pool(processes = self.processes)
        return_list = pool.map(self._contractor, all_para_combination)
        pool.close()

        result_df = self._store_backtest_result_df(return_list, result_dict, para_dict_key_list, manager_list)
        self._draw_graphs_and_tables(result_df, para_dict_key_list)

        # return_list = self._get_return_list(all_para_combination)
        # print(strategy, symbol, interval, category, exchange, asset, "action = finished full backtest")
        # print(strategy, symbol, interval, category, exchange, asset, "action = exported backtest_report to csv")
        # print(strategy, symbol, interval, category, exchange, asset, "action = created sharpe ratio distribution table")

    def _get_para_dict(self):

        para_dict = {
            "rolling_window" : [10, 20, 30], # rw cannot be 0
            "upper_band"     : [0, 1, 2, 3, 4],
            "lower_band"     : [0, 1, 2, 3, 4]
        }
        """
        para_dict = {
            "rolling_window" : [10, 20, 30, 40, 50, 60, 70, 80, 90, 100], # rw cannot be 0
            "upper_band"     : [0, 0.1, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4, 4.25, 4.5, 4.75, 5],
            "lower_band"     : [0, 0.1, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4, 4.25, 4.5, 4.75, 5]
        }
        """

        return para_dict

    def _get_manager_list(self):
        manager_list = mp.Manager().list()

        return manager_list

    def _get_para_list(self, para_dict):
        para_list = list(para_dict.values())

        return para_list

    def _get_all_para_combination(self, para_list, single_df, manager_list):
        all_para_combination = list(itertools.product(*para_list))

        for i in range(len(all_para_combination)):
            para_combination = all_para_combination[i]
            para_combination = list(para_combination)

            para_combination.append(single_df)
            para_combination.append(manager_list)

            all_para_combination[i] = para_combination

        return all_para_combination

    def _get_result_dict(self, para_dict):
        result_dict = {}

        for para_dict_key in para_dict:
            result_dict[para_dict_key] = []

        result_dict["num_of_long_trade"]  = []
        result_dict["num_of_short_trade"] = []
        result_dict["num_of_trade"]       = []

        result_dict["strat_win_rate"]   = []
        result_dict["strat_ann_return"] = []
        result_dict["strat_mdd"]        = []
        result_dict["strat_calmar"]     = []
        result_dict["strat_sharpe"]     = []

        result_dict["long_sharpe"]  = []
        result_dict["short_sharpe"] = []

        result_dict["long_win_rate"]   = []
        result_dict["long_ann_return"] = []
        result_dict["long_mdd"]        = []
        result_dict["long_calmar"]     = []

        result_dict["short_win_rate"]   = []
        result_dict["short_ann_return"] = []
        result_dict["short_mdd"]        = []
        result_dict["short_calmar"]     = []

        return result_dict

    def _get_para_dict_key_list(self, result_dict):
        para_dict_key_list = list(result_dict)

        return para_dict_key_list

    def _store_backtest_result_df(self, return_list, result_dict, para_dict_key_list, manager_list):
        finished_path = self.finished_path

        symbol = self.symbol

        for return_list in manager_list:
            for i in range(len(return_list)):
                result = return_list[i]
                result_dict[para_dict_key_list[i]].append(result)

        full_result_path = f"{finished_path}/{symbol}/full_result"
        self._create_folder(full_result_path)

        result_df  = pd.DataFrame(result_dict)
        result_df  = result_df.sort_values(by = "strat_sharpe", ascending = False)
        result_csv = f"{full_result_path}/{symbol}.csv"
        result_df.to_csv(result_csv, index = False)

        return result_df

    def _contractor(self, para_combination):
        if len(para_combination) > 1:
            manager_list = para_combination[-1]
            manager_list.append(self.backtest(para_combination[0:-1]))

    def backtest(self, para_combination):
        base_csv_existed, base_csv = self._check_base_csv(para_combination)

        if base_csv_existed == False:
            backtest_ready_list = self.get_backtest_ready_list_without_base_csv(para_combination)

        else:
            backtest_ready_list = self.get_backtest_ready_list_with_base_csv(base_csv, para_combination) # start_int and end_int issue

        """
        df_list = []

        df_list.append(df)
        df_list.append(training_df)
        df_list.append(testing_df)
        """

        # for backtest_df in df_list: # the bt_ready is not same as the backtest_df
        single_result_list = self.get_single_result_list(backtest_ready_list, base_csv_existed, para_combination)
        single_result_df   = self._get_single_result_df(single_result_list)
        self._store_single_result_df(single_result_df, para_combination)

        return_list = self._calculate_performance_matrix(single_result_list, para_combination)

        return return_list

    def _check_base_csv(self, para_combination):
        rolling_window = para_combination[0]
        upper_band     = para_combination[1]
        lower_band     = para_combination[2]

        base_csv_existed = False

        base_path = f"{self.finished_path}/{self.symbol}/single_result"
        base_csv  = f"{base_path}/{self.symbol}_{rolling_window}_{upper_band}_{lower_band}.csv"

        if os.path.isfile(base_csv) == True:
            base_csv_existed = True

        return base_csv_existed, base_csv

    def get_backtest_ready_list_without_base_csv(self, para_combination):
        single_df["ma"]      = self.single_df["fr_oi"].rolling(self.rolling_window).mean()
        single_df["sd"]      = self.single_df["fr_oi"].rolling(self.rolling_window).std()
        single_df["z_score"] = (self.single_df["fr_oi"] - self.single_df["ma"]) / self.single_df["sd"]
        # df.dropna(inplace = True)

        long_pos_opened  = False
        short_pos_opened = False

        signal_list            = []
        long_trading_pnl_list  = []
        short_trading_pnl_list = []
        long_fr_pnl_list       = []
        short_fr_pnl_list      = []
        long_tx_fee_list       = []
        short_tx_fee_list      = []

        backtest_ready_list = []

        backtest_ready_list.append(long_pos_opened)
        backtest_ready_list.append(short_pos_opened)
        backtest_ready_list.append(signal_list)
        backtest_ready_list.append(long_trading_pnl_list)
        backtest_ready_list.append(short_trading_pnl_list)
        backtest_ready_list.append(long_fr_pnl_list)
        backtest_ready_list.append(short_fr_pnl_list)
        backtest_ready_list.append(long_tx_fee_list)
        backtest_ready_list.append(short_tx_fee_list)

        return backtest_ready_list

    def get_backtest_ready_list_with_base_csv(self, base_csv, para_combination):
        base_df = pd.read_csv(base_csv)

        signal = base_df["signal"].iloc[-1]

        if signal == 1:
            long_pos_opened  = True
            short_pos_opened = False

        elif signal == 0:
            long_pos_opened  = False
            short_pos_opened = False

        elif signal == -1:
            long_pos_opened  = False
            short_pos_opened = True

        signal_list            = base_df["signal"].tolist()
        long_trading_pnl_list  = base_df["long_trading_pnl"].tolist()
        short_trading_pnl_list = base_df["short_trading_pnl"].tolist()
        long_fr_pnl_list       = base_df["long_fr_pnl"].tolist()
        short_fr_pnl_list      = base_df["short_fr_pnl"].tolist()
        long_tx_fee_list       = base_df["long_tx_fee"].tolist()
        short_tx_fee_list      = base_df["short_tx_fee"].tolist()

        backtest_ready_list = []

        backtest_ready_list.append(long_pos_opened)
        backtest_ready_list.append(short_pos_opened)
        backtest_ready_list.append(signal_list)
        backtest_ready_list.append(long_trading_pnl_list)
        backtest_ready_list.append(short_trading_pnl_list)
        backtest_ready_list.append(long_fr_pnl_list)
        backtest_ready_list.append(short_fr_pnl_list)
        backtest_ready_list.append(long_tx_fee_list)
        backtest_ready_list.append(short_tx_fee_list)

        return backtest_ready_list

    def get_single_result_list(self, backtest_ready_list, base_csv_existed, para_combination):
        # df = backtest_df
        single_df = self.single_backtest_df

        asset    = self.asset
        strategy = self.strategy
        exchange = self.exchange
        category = self.category
        interval = self.interval
        symbol   = self.symbol

        rolling_window = para_combination[0]
        upper_band     = para_combination[1]
        lower_band     = para_combination[2]

        open_price        = 0
        close_price       = 0
        signal            = 0
        long_trading_pnl  = 0
        short_trading_pnl = 0
        long_fr_pnl       = 0
        short_fr_pnl      = 0
        long_tx_fee       = 0
        short_tx_fee      = 0

        long_pos_opened        = backtest_ready_list[0]
        short_pos_opened       = backtest_ready_list[1]
        signal_list            = backtest_ready_list[2]
        long_trading_pnl_list  = backtest_ready_list[3]
        short_trading_pnl_list = backtest_ready_list[4]
        long_fr_pnl_list       = backtest_ready_list[5]
        short_fr_pnl_list      = backtest_ready_list[6]
        long_tx_fee_list       = backtest_ready_list[7]
        short_tx_fee_list      = backtest_ready_list[8]

        single_result_list = []

        loop_len = len(single_df) - len(signal_list)

        for i in range(loop_len):
            now_price   = single_df.loc[i, "open"]
            now_fr      = single_df.loc[i, "funding_rate"]
            now_z_score = single_df.loc[i, "z_score"]

            # S1: 0 position -> no signal triggered
            if ((long_pos_opened == False) and (short_pos_opened == False)) and ((now_z_score <= upper_band) and (now_z_score >= -1 * lower_band)):
                logn_pos_opened  = False
                short_pos_opened = False

                signal = 0
                signal_list.append(signal)

                long_trading_pnl = 0
                long_trading_pnl_list.append(long_trading_pnl)

                short_trading_pnl = 0
                short_trading_pnl_list.append(short_trading_pnl)

                long_fr_pnl = 0
                long_fr_pnl_list.append(long_fr_pnl)

                short_fr_pnl = 0
                short_fr_pnl_list.append(short_fr_pnl)

                long_tx_fee = 0
                long_tx_fee_list.append(long_tx_fee)

                short_tx_fee = 0
                short_tx_fee_list.append(short_tx_fee)

            # S2: 0 position -> long signal triggered
            elif ((long_pos_opened == False) and (short_pos_opened == False)) and (now_z_score < -1 * lower_band):
                long_pos_opened  = True
                short_pos_opened = False

                signal = 1
                signal_list.append(signal)

                open_price  = now_price

                long_trading_pnl = 0
                long_trading_pnl_list.append(long_trading_pnl)

                short_trading_pnl = 0
                short_trading_pnl_list.append(short_trading_pnl)

                long_fr_pnl = 0
                long_fr_pnl_list.append(long_fr_pnl)

                short_fr_pnl = 0
                short_fr_pnl_list.append(short_fr_pnl)

                long_tx_fee = self.binance_tx_fee_rate
                long_tx_fee_list.append(long_tx_fee)

                short_tx_fee = 0
                short_tx_fee_list.append(short_tx_fee)

            # S3: 0 position -> short signal triggered
            elif ((long_pos_opened == False) and (short_pos_opened == False)) and (now_z_score > upper_band):
                long_pos_opened  = False
                short_pos_opened = True

                signal = -1
                signal_list.append(signal)

                open_price = now_price

                long_trading_pnl = 0
                long_trading_pnl_list.append(long_trading_pnl)

                short_trading_pnl = 0
                short_trading_pnl_list.append(short_trading_pnl)

                long_fr_pnl = 0
                long_fr_pnl_list.append(long_fr_pnl)

                short_fr_pnl = 0
                short_fr_pnl_list.append(short_fr_pnl)

                long_tx_fee = 0
                long_tx_fee_list.append(long_tx_fee)

                short_tx_fee = self.binance_tx_fee_rate
                short_tx_fee_list.append(short_tx_fee)

            # S4: long position -> long signal ended but not trigger short signal
            elif ((long_pos_opened == True) and (short_pos_opened == False)) and (-1 * lower_band <= now_z_score <= upper_band):
                long_pos_opened  = False
                short_pos_opened = False

                signal = 0
                signal_list.append(signal)

                close_price = now_price

                long_trading_pnl = (close_price - open_price) / open_price
                long_trading_pnl_list.append(long_trading_pnl)

                short_trading_pnl = 0
                short_trading_pnl_list.append(short_trading_pnl)

                long_fr_pnl = now_fr * -1
                long_fr_pnl_list.append(long_fr_pnl)

                short_fr_pnl = 0
                short_fr_pnl_list.append(short_fr_pnl)

                long_tx_fee = self.binance_tx_fee_rate
                long_tx_fee_list.append(long_tx_fee)

                short_tx_fee = 0
                short_tx_fee_list.append(short_tx_fee)

            # S5: short position -> short signal ended but not trigger long signal
            elif ((long_pos_opened == False) and (short_pos_opened == True)) and (upper_band >= now_z_score >= -1 * lower_band):
                long_pos_opened  = False
                short_pos_opened = False

                signal = 0
                signal_list.append(signal)

                close_price = now_price

                short_trading_pnl = (open_price - close_price) / open_price
                short_trading_pnl_list.append(short_trading_pnl)

                long_trading_pnl = 0
                long_trading_pnl_list.append(long_trading_pnl)

                long_fr_pnl = 0
                long_fr_pnl_list.append(long_fr_pnl)

                short_fr_pnl = now_fr * 1
                short_fr_pnl_list.append(short_fr_pnl)

                long_tx_fee = 0
                long_tx_fee_list.append(long_tx_fee)

                short_tx_fee = self.binance_tx_fee_rate
                short_tx_fee_list.append(short_tx_fee)

            # S6: long position -> long signal continue
            elif ((long_pos_opened == True) and (short_pos_opened == False)) and (now_z_score < -1 * lower_band):
                long_pos_opened  = True
                short_pos_opened = False

                signal = 1
                signal_list.append(signal)

                close_price = now_price

                long_trading_pnl = (close_price - open_price) / open_price
                long_trading_pnl_list.append(long_trading_pnl)

                short_trading_pnl = 0
                short_trading_pnl_list.append(short_trading_pnl)

                long_fr_pnl = now_fr * -1
                long_fr_pnl_list.append(long_fr_pnl)

                short_fr_pnl = 0
                short_fr_pnl_list.append(short_fr_pnl)

                long_tx_fee = 0
                long_tx_fee_list.append(long_tx_fee)

                short_tx_fee = 0
                short_tx_fee_list.append(short_tx_fee)

                open_price = now_price

            # S7: short position -> short signal continue
            elif ((long_pos_opened == False) and (short_pos_opened == True)) and (now_z_score > upper_band):
                long_pos_opened  = False
                short_pos_opened = True

                signal = -1
                signal_list.append(signal)

                close_price = now_price

                short_trading_pnl = (open_price - close_price) / open_price
                short_trading_pnl_list.append(short_trading_pnl)

                long_trading_pnl = 0
                long_trading_pnl_list.append(long_trading_pnl)

                long_fr_pnl = 0
                long_fr_pnl_list.append(long_fr_pnl)

                short_fr_pnl = now_fr * 1
                short_fr_pnl_list.append(short_fr_pnl)

                long_tx_fee = 0
                long_tx_fee_list.append(long_tx_fee)

                short_tx_fee = 0
                short_tx_fee_list.append(short_tx_fee)

                open_price = now_price

            # S8: long position -> long signal becomes short signal
            elif ((long_pos_opened == True) and (short_pos_opened == False)) and (now_z_score > upper_band):
                long_pos_opened  = False
                short_pos_opened = True

                signal = -1
                signal_list.append(signal)

                close_price = now_price

                long_trading_pnl = (close_price - open_price) / open_price
                long_trading_pnl_list.append(long_trading_pnl)

                short_trading_pnl = 0
                short_trading_pnl_list.append(short_trading_pnl)

                long_fr_pnl = now_fr * -1
                long_fr_pnl_list.append(long_fr_pnl)

                short_fr_pnl = 0
                short_fr_pnl_list.append(short_fr_pnl)

                long_tx_fee = self.binance_tx_fee_rate
                long_tx_fee_list.append(long_tx_fee)

                short_tx_fee = 0
                short_tx_fee_list.append(short_tx_fee)

                open_price = now_price

            # S9: short position -> short signal becomes long signal
            elif ((long_pos_opened == False) and (short_pos_opened == True)) and (now_z_score < -1 * lower_band):
                long_pos_opened  = True
                short_pos_opened = False

                signal = 1
                signal_list.append(signal)

                close_price = now_price

                short_trading_pnl = (open_price - close_price) / open_price
                short_trading_pnl_list.append(short_trading_pnl)

                long_trading_pnl = 0
                long_trading_pnl_list.append(long_trading_pnl)

                long_fr_pnl = 0
                long_fr_pnl_list.append(long_fr_pnl)

                short_fr_pnl = now_fr * 1
                short_fr_pnl_list.append(short_fr_pnl)

                long_tx_fee = 0
                long_tx_fee_list.append(long_tx_fee)

                short_tx_fee = self.binance_tx_fee_rate
                short_tx_fee_list.append(short_tx_fee)

                open_price = now_price

            # S10: no signal during the rolling window period
            elif i <= (rolling_window - 2) and (base_csv_existed == False): # for 1st time backtest only
                signak = 0
                signal_list.append(signal)

                long_trading_pnl = 0
                long_trading_pnl_list.append(long_trading_pnl)

                short_trading_pnl = 0
                short_trading_pnl_list.append(short_trading_pnl)

                long_fr_pnl = 0
                long_fr_pnl_list.append(long_fr_pnl)

                short_fr_pnl = 0
                short_fr_pnl_list.append(short_fr_pnl)

                long_tx_fee = 0
                long_tx_fee_list.append(long_tx_fee)

                short_tx_fee = 0
                short_tx_fee_list.append(short_tx_fee)

        print(strategy, symbol, interval, category, exchange, asset, "action = finished backtest", "(", rolling_window, upper_band, lower_band, ")", )

        single_result_list.append(single_df)
        single_result_list.append(signal_list)
        single_result_list.append(long_trading_pnl_list)
        single_result_list.append(short_trading_pnl_list)
        single_result_list.append(long_fr_pnl_list)
        single_result_list.append(short_fr_pnl_list)
        single_result_list.append(long_tx_fee_list)
        single_result_list.append(short_tx_fee_list)

        return single_result_list

    def _get_single_result_df(self, single_result_list):
        single_df              = single_result_list[0]
        signal_list            = single_result_list[1]
        long_trading_pnl_list  = single_result_list[2]
        short_trading_pnl_list = single_result_list[3]
        long_fr_pnl_list       = single_result_list[4]
        short_fr_pnl_list      = single_result_list[5]
        long_tx_fee_list       = single_result_list[6]
        short_tx_fee_list      = single_result_list[7]

        signal_df            = pd.DataFrame(signal_list)
        long_trading_pnl_df  = pd.DataFrame(long_trading_pnl_list)
        short_trading_pnl_df = pd.DataFrame(short_trading_pnl_list)
        long_fr_pnl_df       = pd.DataFrame(long_fr_pnl_list)
        short_fr_pnl_df      = pd.DataFrame(short_fr_pnl_list)
        long_tx_fee_df       = pd.DataFrame(long_tx_fee_list)
        short_tx_fee_df      = pd.DataFrame(short_tx_fee_list)

        single_df_col_list   = single_df.columns.tolist()
        append_col_list      = ["signal",
                    "long_trading_pnl", "short_trading_pnl",
                    "long_fr_pnl", "short_fr_pnl",
                    "long_tx_fee", "short_tx_fee"]

        single_df_col_list.extend(append_col_list)

        single_result_df = pd.concat((single_df, signal_df,
                           long_trading_pnl_df, short_trading_pnl_df,
                           long_fr_pnl_df, short_fr_pnl_df,
                           long_tx_fee_df, short_tx_fee_df), axis = 1)

        single_result_df.columns = single_df_col_list

        return single_result_df

    def _store_single_result_df(self, single_result_df, para_combination):
        rolling_window = para_combination[0]
        upper_band     = para_combination[1]
        lower_band     = para_combination[2]

        result_path = f"{self.finished_path}/{self.symbol}"
        self._create_folder(result_path)

        single_result_path = f"{result_path}/single_result"
        self._create_folder(single_result_path)

        single_result_csv = f"{single_result_path}/{self.symbol}_{rolling_window}_{upper_band}_{lower_band}.csv"
        single_result_df.to_csv(single_result_csv, index = False)

    def _calculate_performance_matrix(self, single_result_list, para_combination):
        binance_tx_fee_rate = self.binance_tx_fee_rate

        signal_list = single_result_list[1]

        long_trading_pnl_list  = single_result_list[2]
        short_trading_pnl_list = single_result_list[3]

        long_fr_pnl_list  = single_result_list[4]
        short_fr_pnl_list = single_result_list[5]

        long_tx_fee_list  = single_result_list[6]
        short_tx_fee_list = single_result_list[7]

        long_trading_fr_pnl_list  = np.add(long_trading_pnl_list, long_fr_pnl_list)
        short_trading_fr_pnl_list = np.add(short_trading_pnl_list, short_fr_pnl_list)
        strat_trading_pnl_list    = np.add(long_trading_pnl_list, short_trading_pnl_list)

        long_full_pnl_list  = np.subtract(long_trading_fr_pnl_list, long_tx_fee_list)
        short_full_pnl_list = np.subtract(short_trading_fr_pnl_list, short_tx_fee_list)

        strat_full_pnl_list = np.add(long_full_pnl_list, short_full_pnl_list)

        strat_dd_list = self._get_dd_list(strat_full_pnl_list)
        long_dd_list  = self._get_dd_list(long_full_pnl_list)
        short_dd_list = self._get_dd_list(short_full_pnl_list)

        num_of_long_trade  = signal_list.count(1)
        num_of_short_trade = signal_list.count(-1)
        num_of_trade       = num_of_long_trade + num_of_short_trade

        strat_pos_pnl_day = [num for num in strat_full_pnl_list if num > 0]
        strat_neg_pnl_day = [num for num in strat_full_pnl_list if num < -1 * self.binance_tx_fee_rate]

        long_pos_pnl_day = [num for num in long_full_pnl_list if num > 0]
        long_neg_pnl_day = [num for num in long_full_pnl_list if num < -1 * self.binance_tx_fee_rate]

        short_pos_pnl_day = [num for num in short_full_pnl_list if num > 0]
        short_neg_pnl_day = [num for num in short_full_pnl_list if num < -1 * self.binance_tx_fee_rate]

        strat_win_rate = self._calculate_win_rate(strat_pos_pnl_day, strat_neg_pnl_day)
        long_win_rate  = self._calculate_win_rate(long_pos_pnl_day, long_neg_pnl_day)
        short_win_rate = self._calculate_win_rate(short_pos_pnl_day, short_neg_pnl_day)

        strat_ann_return = np.around(np.mean(strat_full_pnl_list) * self.ann_multiple, decimals = 4)
        long_ann_return  = np.around(np.mean(long_full_pnl_list) * self.ann_multiple, decimals = 4)
        short_ann_return = np.around(np.mean(short_full_pnl_list) * self.ann_multiple, decimals = 4)

        strat_mdd = self._calculate_mdd(strat_dd_list)
        long_mdd  = self._calculate_mdd(long_dd_list)
        short_mdd = self._calculate_mdd(short_dd_list)

        strat_calmar = self._calculate_calmar_ratio(strat_ann_return, strat_mdd)
        long_calmar  = self._calculate_calmar_ratio(long_ann_return, long_mdd)
        short_calmar = self._calculate_calmar_ratio(short_ann_return, short_mdd)

        strat_sharpe = self._calculate_sharpe_ratio(strat_full_pnl_list)
        long_sharpe  = self._calculate_sharpe_ratio(long_full_pnl_list)
        short_sharpe = self._calculate_sharpe_ratio(short_full_pnl_list)

        return_list = []

        rolling_window = para_combination[0]
        upper_band     = para_combination[1]
        lower_band     = para_combination[2]

        return_list.append(rolling_window)
        return_list.append(upper_band)
        return_list.append(lower_band)

        return_list.append(num_of_long_trade)
        return_list.append(num_of_short_trade)
        return_list.append(num_of_trade)

        return_list.append(strat_win_rate)
        return_list.append(strat_ann_return)
        return_list.append(strat_mdd)
        return_list.append(strat_calmar)
        return_list.append(strat_sharpe)
        return_list.append(long_sharpe)
        return_list.append(short_sharpe)

        return_list.append(long_win_rate)
        return_list.append(long_ann_return)
        return_list.append(long_mdd)
        return_list.append(long_calmar)

        return_list.append(short_win_rate)
        return_list.append(short_ann_return)
        return_list.append(short_mdd)
        return_list.append(short_calmar)

        return return_list
    def _get_dd_list(self, pnl_list):
        cum_pnl_list = np.cumsum(pnl_list)

        dd_list = []

        for i in range(len(cum_pnl_list)):
            pnl = cum_pnl_list[i]

            if i == 0:
                temp_max = pnl

            else:
                if pnl > temp_max:
                    temp_max = pnl

            dd = pnl - temp_max
            dd_list.append(dd)

        return dd_list

    def _calculate_win_rate(self, pos_pnl_day, neg_pnl_day):
        try:
            win_rate = np.around(len(pos_pnl_day) / (len(pos_pnl_day) + len(neg_pnl_day)), decimals = 4)

        except ZeroDivisionError:
            win_rate = 0

        return win_rate

    def _calculate_mdd(self, dd_list):
        try:
            mdd = np.around(np.min(dd_list), decimals = 4)

        except ValueError:
            mdd = 0

        return mdd

    def _calculate_calmar_ratio(self, ann_return, mdd):
        if mdd != 0:
            calmar = np.around(ann_return / abs(mdd), decimals = 2)

        else:
            calmar = 0

        return calmar

    def _calculate_sharpe_ratio(self, pnl_list):
        ann_multiple = self.ann_multiple
        
        if np.std(pnl_list) != 0:
            sharpe = np.around(np.mean(pnl_list) / np.std(pnl_list) * math.sqrt(ann_multiple), decimals = 2)

        else:
            sharpe = 0

        return sharpe

    def _create_folder(self, folder):
        """ This function is used to create the folder from the path.

        Args:
            folder (str): path of the file
        """

        if os.path.isdir(folder) == False:
            os.mkdir(folder)

    def _get_sharpe_table_path(self, para_a, para_b):
        finished_path = self.finished_path

        symbol = self.symbol

        sharpe_table_path = f"{finished_path}/{symbol}/full_result/{symbol}_{para_a}_{para_b}.png"

        return sharpe_table_path

    def _draw_graphs_and_tables(self, result_df, para_dict_key_list):
        para_a = para_dict_key_list[0]
        para_b = para_dict_key_list[1]
        para_c = para_dict_key_list[2]

        table_dict = {}
        table_list = []

        table_ab = result_df[(result_df[para_c] == 1.75)]
        table_ab = table_ab.pivot(index = para_a, columns = para_b, values = "strat_sharpe")
        table_dict["table"]  = table_ab
        table_dict["para_a"] = para_a
        table_dict["para_b"] = para_b
        table_list.append(table_dict)

        table_dict = {}
        table_ac = result_df[(result_df[para_b] == 4)]
        table_ac = table_ac.pivot(index = para_a, columns = para_c, values = "strat_sharpe")
        table_dict["table"]  = table_ac
        table_dict["para_a"] = para_a
        table_dict["para_b"] = para_c
        table_list.append(table_dict)

        table_dict = {}
        table_bc = result_df[(result_df[para_a] == 10)]
        table_bc = table_bc.pivot(index = para_b, columns = para_c, values = "strat_sharpe")
        table_dict["table"]  = table_bc
        table_dict["para_a"] = para_b
        table_dict["para_b"] = para_c
        table_list.append(table_dict)

        for table_dict in table_list:
            self._draw_sharpe_distribution_table(table_dict)

    def _draw_sharpe_distribution_table(self, table_dict):
        table  = table_dict["table"]
        para_x = table_dict["para_a"]
        para_y = table_dict["para_b"]

        plt.figure(figsize = (20, 8))
        plt.imshow(table, cmap = "PuBu", aspect = "auto")
        plt.colorbar()
        plt.title("Sharpe Ratio Distribution")
        plt.xlabel(para_y)
        plt.ylabel(para_x)
        plt.xticks(range(len(table.columns)), table.columns, rotation = "vertical")
        plt.yticks(range(len(table.index)), table.index)

        for i in range(len(table.index)):
            for j in range(len(table.columns)):
                plt.text(j, i, f"{table.iloc[i, j]:.2f}", ha = "center", va = "center", color = "black")

        sharpe_table_path = self._get_sharpe_table_path(para_x, para_y)
        plt.savefig(sharpe_table_path)
        # plt.show()

        """
        # PuBu -> Blue, Yellow, for investors
        # BuGn -> Green, for investors
    
        # RdYlGn -> Green, Yellow, Red -> for myself
        # YlGn   -> Green, Yellow      -> for myself
    
        self.create_sharpe_ratio_surface(result_df, para1, para2, para3, "SR")
        """

    """
    def create_sharpe_ratio_surface(self, result_df, x_para, y_para, z_para, title):
        x_values     = result_df[x_para]
        y_values     = result_df[y_para]
        z_values     = result_df[z_para]
        sharpe_ratio = result_df["strat_sharpe"]

        fig = plt.figure()
        ax  = fig.add_subplot(111, projection = "3d")

        sc = ax.scatter(x_values, y_values, z_values, c = sharpe_ratio, cmap = "viridis")

        ax.set_xlabel(x_para)
        ax.set_ylabel(y_para)
        ax.set_zlabel(z_para)
        ax.set_title(title)
        plt.colorbar(sc, label = "Sharpe Ratio")

        plt.show()
        """

class DataProcessor:
    def __init__(self, backtest_df_ready = False, symbol = None):
        self.asset      = "Cryptocurrency"
        self.strategy   = "bybit_funding_rate_open_interest"
        self.exchange   = "bybit"
        self.price_func = "kline"
        self.fr_func    = "funding_rate"
        self.oi_func    = "open_interest"
        self.category   = "linear"
        self.interval   = "240" # 4h for open_interest
        self.symbol     = symbol

        self.start_dt = datetime.datetime(2020, 1, 1, 0, 0, 0)
        self.end_dt   = datetime.datetime(2024, 1, 1, 0, 0, 0)

        self.seconds_to_ms = 1000
        self.start_int     = calendar.timegm(self.start_dt.timetuple()) * self.seconds_to_ms
        self.end_int       = calendar.timegm(self.end_dt.timetuple()) * self.seconds_to_ms

        self.backtest_df_ready = backtest_df_ready

    def put_data_into_backtest_system(self):
        backtest_df   = self.get_formatted_backtest_df()
        finished_path = self.get_finished_path()
        symbol        = self.symbol

        backtestSystem = BacktestSystem(backtest_df, finished_path, symbol)
        backtestSystem.run_backtest()

    def _create_folder(self, folder):
        """ This function is used to create the folder from the path.

        Args:
            folder (str): path of the file
        """

        if os.path.isdir(folder) == False:
            os.mkdir(folder)

    def _create_folders(self):
        asset    = self.asset
        strategy = self.strategy
        exchange = self.exchange
        function = self.price_func
        category = self.category
        interval = self.interval

        base_folder = "D:\\backtest\\" + asset
        self._create_folder(base_folder)

        zero_folder = f"{base_folder}/{strategy}"
        self._create_folder(zero_folder)

        first_folder = f"{zero_folder}/{exchange}"
        self._create_folder(first_folder)

        second_folder = f"{first_folder}/{function}"
        self._create_folder(second_folder)

        third_folder = f"{second_folder}/{category}"
        self._create_folder(third_folder)

        fourth_folder = f"{third_folder}/{interval}"
        self._create_folder(fourth_folder)

    def _get_symbol_list(self):
        symbol_list_path = self._get_price_path()
        symbol_list      = self._get_file_list(symbol_list_path)

        return symbol_list

    def _get_finished_list(self):
        finished_path = self.get_finished_path()

        try:
            finished_list = self._get_file_list(finished_path)

        except FileNotFoundError:
            finished_list = []

            self._create_folders()

        return finished_list

    def get_finished_path(self):
        asset    = self.asset
        strategy = self.strategy
        exchange = self.exchange
        function = self.price_func
        category = self.category
        interval = self.interval

        finished_path = f"D:/backtest/{asset}/{strategy}/{exchange}/{function}/{category}/{interval}"

        return finished_path

    def _get_price_df(self):
        symbol = self.symbol

        start_int = self.start_int
        end_int   = self.end_int

        price_path = self._get_price_path()
        price_csv  = f"{price_path}/{symbol}.csv"

        price_df = pd.read_csv(price_csv)
        price_df = price_df[["time", "datetime", "open"]]
        price_df = price_df.round({"open_time": -3})
        price_df.rename(columns = {"open_time": "time"}, inplace = True)

        price_df = price_df[(price_df["time"] >= start_int) & (price_df["time"] <= end_int)]

        return price_df

    def _get_price_path(self):
        asset    = self.asset
        exchange = self.exchange
        function = self.price_func
        category = self.category
        interval = self.interval

        price_path = f"D:/{asset}/{exchange}/{function}/{category}/{interval}"

        return price_path

    def _get_funding_rate_df(self):
        asset    = self.asset
        strategy = self.strategy
        exchange = self.exchange
        function = self.fr_func
        category = self.category
        interval = self.interval
        symbol   = self.symbol

        funding_rate_path = self._get_funding_rate_path()
        funding_rate_csv  = f"{funding_rate_path}/{symbol}.csv"

        try:
            funding_rate_df = pd.read_csv(funding_rate_csv)
            funding_rate_df = funding_rate_df[["time", "datetime", "funding_rate"]]
            funding_rate_df = funding_rate_df.round({"time": -3})

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "category": category, "function": function, "exchange": exchange, "asset": asset,  "msg": "no funding_rate data file"}

            if symbol[-6:].isdigit() == True:
                pass

            else:
                message = str(message)
                self._send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, category, function, exchange, asset, "response = failed backtest, reason = without funding_rate data file")
            print("**************************************************")

            raise StopIteration

        return funding_rate_df

    def _get_funding_rate_path(self):
        asset    = self.asset
        exchange = self.exchange
        function = self.fr_func
        category = self.category
        interval = "480"

        funding_rate_path = f"D:/{asset}/{exchange}/{function}/{category}/{interval}"

        return funding_rate_path

    def get_open_interest_df(self):
        asset    = self.asset
        strategy = self.strategy
        exchange = self.exchange
        function = self.oi_func
        category = self.category
        interval = "4h"
        symbol   = self.symbol

        open_interest_path = self.get_open_interest_path()
        open_interest_csv  = f"{open_interest_path}/{symbol}.csv"

        try:
            open_interest_df = pd.read_csv(open_interest_csv)
            open_interest_df = open_interest_df[["time", "datetime", "open_interest"]]
            open_interest_df = open_interest_df.round({"time": -3})
            open_interest_df.rename(columns = {"open": "openInterest"}, inplace = True)

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "category": category, "function": function, "exchange": exchange, "asset": asset, "msg": "no open_interest data file"}

            if symbol[-6:].isdigit() == True:
                pass

            else:
                message = str(message)
                self._send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, category, function, exchange, asset, "response = failed backtest, reason = without open_interest data file")
            print("**************************************************")

            raise StopIteration

        return open_interest_df

    def get_open_interest_path(self):
        asset    = self.asset
        exchange = self.exchange
        function = self.oi_func
        category = self.category
        interval = "4h"

        open_interest_path = f"D:/{asset}/{exchange}/{function}/{category}/{interval}"

        return open_interest_path

    def _get_price_fr_df(self):
        funding_rate_df = self._get_funding_rate_df()
        price_df        = self._get_price_df()

        price_fr_df = pd.merge(funding_rate_df, price_df, on = "time", how = "inner")
        price_fr_df.rename(columns = {"datetime_x": "datetime"}, inplace = True)
        price_fr_df = price_fr_df[["time", "datetime", "open", "funding_rate"]]

        return price_fr_df

    def get_formatted_backtest_df(self):
        open_interest_df = self.get_open_interest_df()
        price_fr_df      = self._get_price_fr_df()

        backtest_df = pd.merge(open_interest_df, price_fr_df, on = "time", how = "inner")
        backtest_df.rename(columns = {"datetime_x": "datetime"}, inplace=True)

        backtest_df["fr_oi"] = backtest_df["funding_rate"] / backtest_df["open_interest"]

        backtest_df = backtest_df[["time", "datetime", "open", "funding_rate", "fr_oi"]]
        backtest_df = self._clean_data(backtest_df)

        return backtest_df

    def _clean_data(self, df):
        df.dropna(inplace = True)  # drop Nan value
        # df = df[df != 0].dropna()  # drop "0" value
        df = df.reset_index(drop = True)  # reset row index

        return df

    def _send_tg_msg_to_backtest_channel(self, message):
        base_url = "https://api.telegram.org/bot6233469935:AAHayu1tVZ4NleqRFM-61F6VQObWMCwF90U/sendMessage?chat_id=-809813823&text="
        requests.get(base_url + message)

    def _get_file_list(self, path):
        file_list = []

        for file in os.listdir(path):
            if os.path.isfile(os.path.join(path, file)):
                file = os.path.splitext(file)[0]
                file_list.append(file)

            else:
                file_list.append(file)

        return file_list

def main():
    dataProcessor = DataProcessor()

    symbol_list   = dataProcessor._get_symbol_list()
    finished_list = dataProcessor._get_finished_list()

    symbol_list   = ["BTCUSDT"]
    # finished_list = []

    for symbol in symbol_list:
        if symbol not in finished_list:
            backtest_df_ready = False

        else:
            backtest_df_ready = True

        try:
            dataProcessor = DataProcessor(backtest_df_ready, symbol)
            dataProcessor.put_data_into_backtest_system()

        except StopIteration:
            pass

if __name__ == "__main__":
    main()