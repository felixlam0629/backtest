import calendar
import datetime
import itertools
import math
import multiprocessing as mp
import numpy as np
import os
import pandas as pd
from pprint import pprint
import requests

class BybitBacktestSystem():
    def __init__(self, strategy, category, interval, symbol,
                 finished_path, single_backtest_df,
                 first_round_backtest, full_para_backtest):

        self.asset    = "Cryptocurrency"
        self.exchange = "bybit"
        self.strategy = strategy
        self.category = category
        self.interval = interval
        self.symbol   = symbol

        self.binance_tx_fee_rate = 0.0002
        self.ann_multiple        = 365 * 3

        self.processes = 8

        self.finished_path = finished_path

        self.df_split_index     = int(0.8 * len(single_backtest_df))
        self.single_backtest_df = single_backtest_df

        self.first_round_backtest = first_round_backtest
        self.full_para_backtest   = full_para_backtest

    def _run_full_backtest_system(self):
        single_df_list  = self._get_single_df_list()
        para_dict       = self._get_para_dict()
        para_value_list = self._get_para_value_list(para_dict)

        for single_df_dict in single_df_list:
            single_df_name = list(single_df_dict)[0]

            manager_list         = self._get_manager_list()
            result_dict          = self._get_result_dict(para_dict)
            result_key_list      = self._get_result_key_list(result_dict)
            all_para_combination = self._get_all_para_combination(single_df_dict, para_value_list, manager_list)

            pool        = mp.Pool(processes = self.processes)
            return_list = pool.map(self._contractor, all_para_combination)
            pool.close()
            print(f"{self.exchange}丨{self.strategy}丨{self.category}丨{self.interval}丨{self.symbol}丨{single_df_name}丨action = finished full backtest")

            full_result_df = self._store_full_result_df(return_list, result_dict, result_key_list, single_df_dict, manager_list)
            print(f"{self.exchange}丨{self.strategy}丨{self.category}丨{self.interval}丨{self.symbol}丨{single_df_name}丨action = exported backtest_report to csv")

        print(f"{self.exchange}丨{self.strategy}丨{self.category}丨{self.interval}丨{self.symbol}丨action = finished backtest")
        print("**************************************************")

    def _get_single_df_list(self):
        single_df_list = []

        if self.first_round_backtest == True:
            single_df_dict = {}
            single_df_dict["backtest_set"] = None
            single_df_list.append(single_df_dict)

        elif self.first_round_backtest == False:
            single_df_dict = {}
            single_df_dict["training_set"] = None
            single_df_list.append(single_df_dict)

            single_df_dict = {}
            single_df_dict["testing_set"]  = None
            single_df_list.append(single_df_dict)

        return single_df_list

    def _get_para_dict(self):
        if self.full_para_backtest == False:
            para_dict = {
                "rolling_window" : [10, 20, 30], # rw cannot be 0
                "upper_band"     : [0, 1, 2, 3, 4],
                "lower_band"     : [0, 1, 2, 3, 4]
            }

        else:
            para_dict = {
                "rolling_window" : [10, 20, 30, 40, 50, 60, 70, 80, 90, 100], # rw cannot be 0
                "upper_band"     : [0, 0.1, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3, 3.5, 4],
                "lower_band"     : [0, 0.1, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3, 3.5, 4]
            }

        return para_dict

    def _get_para_value_list(self, para_dict):
        para_value_list = list(para_dict.values())

        return para_value_list

    def _get_manager_list(self):
        manager_list = mp.Manager().list()

        return manager_list

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

    def _get_result_key_list(self, result_dict):
        result_key_list = list(result_dict)

        return result_key_list

    def _get_all_para_combination(self, single_df_dict, para_value_list, manager_list):
        all_para_combination = list(itertools.product(*para_value_list))

        for i in range(len(all_para_combination)):
            para_combination = all_para_combination[i]
            para_combination = list(para_combination)

            rolling_window = para_combination[0]
            single_df_name = list(single_df_dict)[0]

            if single_df_name == "backtest_set":
                single_backtest_df = self.single_backtest_df
                single_df_dict["backtest_set"] = single_backtest_df

            elif single_df_name == "training_set":
                single_training_df = self.single_backtest_df.iloc[:self.df_split_index]
                single_df_dict["training_set"] = single_training_df

            elif single_df_name == "testing_set":
                single_testing_df = self.single_backtest_df.iloc[(self.df_split_index - rolling_window):]
                single_testing_df = single_testing_df.reset_index(drop = True)
                single_df_dict["testing_set"] = single_testing_df

            para_combination.append(single_df_dict)
            para_combination.append(manager_list)

            all_para_combination[i] = para_combination

        return all_para_combination

    def _store_full_result_df(self, return_list, result_dict, result_key_list, single_df_dict, manager_list):
        single_df_name = list(single_df_dict)[0]

        for return_list in manager_list:
            for i in range(len(return_list)):
                result = return_list[i]
                result_dict[result_key_list[i]].append(result)

        full_result_path = f"{self.finished_path}/{self.symbol}/{single_df_name}/full_result"
        self._create_folder(full_result_path)

        full_result_df  = pd.DataFrame(result_dict)
        full_result_df  = full_result_df.sort_values(by = "strat_sharpe", ascending = False)
        full_result_csv = f"{full_result_path}/{self.symbol}.csv"
        full_result_df.to_csv(full_result_csv, index = False)

        return full_result_df

    def _contractor(self, para_combination):
        if len(para_combination) > 1:
            return_list  = []
            manager_list = para_combination[-1]
            return_list  = self._start_single_backtest(para_combination[0:-1])
            manager_list.append(return_list)

    def _start_single_backtest(self, para_combination):
        base_csv_existed, base_csv = self._check_base_csv(para_combination)

        if base_csv_existed == False:
            backtest_ready_list = self._get_backtest_ready_list_without_base_csv(para_combination)

        else:
            backtest_ready_list = self._get_backtest_ready_list_with_base_csv(base_csv, para_combination)

        single_result_list = self._get_single_result_list(backtest_ready_list, base_csv_existed, para_combination)
        single_result_df   = self._get_single_result_df(single_result_list)
        self._store_single_result_df(single_result_df, para_combination)

        return_list = self._calculate_performance_matrix(single_result_list, para_combination)

        return return_list

    def _check_base_csv(self, para_combination):
        rolling_window = para_combination[0]
        upper_band     = para_combination[1]
        lower_band     = para_combination[2]
        single_df_name = list(para_combination[3])[0]

        base_csv_existed = False

        base_path = f"{self.finished_path}/{self.symbol}/single_result/{single_df_name}"
        base_csv  = f"{base_path}/{self.symbol}_{rolling_window}_{upper_band}_{lower_band}.csv"

        if os.path.isfile(base_csv) == True:
            base_csv_existed = True

        return base_csv_existed, base_csv

    def _get_backtest_ready_list_without_base_csv(self, para_combination):
        rolling_window = para_combination[0]
        single_df      = list(para_combination[3].values())[0]

        single_df["ma"]      = single_df["backtest_data"].rolling(rolling_window).mean()
        single_df["sd"]      = single_df["backtest_data"].rolling(rolling_window).std()
        single_df["z_score"] = (single_df["backtest_data"] - single_df["ma"]) / single_df["sd"]

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

    def _get_backtest_ready_list_with_base_csv(self, base_csv, para_combination):
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

    def _get_single_result_list(self, backtest_ready_list, base_csv_existed, para_combination):
        rolling_window = para_combination[0]
        upper_band     = para_combination[1]
        lower_band     = para_combination[2]
        single_df_name = list(para_combination[3])[0]
        single_df      = list(para_combination[3].values())[0]

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

        print(f"{self.exchange}丨{self.strategy}丨{self.category}丨{self.interval}丨{self.symbol}丨{single_df_name}丨({rolling_window}, {upper_band}, {lower_band})丨action = finished backtest")

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
        single_df_name = list(para_combination[3])[0]

        base_result_path = f"{self.finished_path}/{self.symbol}"
        self._create_folder(base_result_path)

        category_result_path = f"{base_result_path}/{single_df_name}"
        self._create_folder(category_result_path)

        single_result_path = f"{category_result_path}/single_result"
        self._create_folder(single_result_path)

        single_result_csv = f"{single_result_path}/{self.symbol}_{rolling_window}_{upper_band}_{lower_band}.csv"
        single_result_df.to_csv(single_result_csv, index = False)

    def _calculate_performance_matrix(self, single_result_list, para_combination):
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

        if os.path.isdir(folder) == False: # unknown bug from mp
            try:
                os.mkdir(folder)

            except FileExistsError:
                pass