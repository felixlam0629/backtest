import datetime
import itertools
import math
import multiprocessing as mp
import numpy as np
import os
import pandas as pd
from pprint import pprint
import requests
import time

class BacktestSystem():
    def __init__(self, df, finished_path, symbol):
        self.df = df

        self.finished_path = finished_path

        self.asset      = "Cryptocurrency"
        self.strategy   = "testing_basis_index_price_open_interest"
        self.exchange   = "binance"
        self.instrument = "futures"
        self.product    = "coinm_futures"
        self.interval   = "8h"
        self.symbol     = symbol

        self.binance_tx_fee_rate = 0.0002
        self.ann_multiple        = 365 * 3

        self.processes = 8

    def run_backtest(self):
        df = self.df

        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        interval   = self.interval
        symbol     = self.symbol

        manager_list = self._get_manager_list()
        para_dict    = self._get_para_dict()

        para_list            = self._get_para_list(para_dict)
        all_para_combination = self._get_all_para_combination(para_list, df, manager_list)

        result_dict        = self._get_result_dict(para_dict)
        para_dict_key_list = self._get_para_dict_key_list(result_dict)

        # return_list = self._get_return_list(all_para_combination)
        processes  = self.processes
        contractor = self.contractor

        pool        = mp.Pool(processes = processes)
        return_list = pool.map(contractor, all_para_combination)
        pool.close()

        print(strategy, symbol, interval, product, instrument, exchange, asset, "action = finished full backtest")

        result_df = self._store_backtest_result_df(return_list, result_dict, para_dict_key_list, manager_list)
        print(strategy, symbol, interval, product, instrument, exchange, asset, "action = exported backtest_report to csv")

        # self.draw_grap_and_table(result_df, para_dict_key_list)

    def draw_grap_and_table(self, result_df, para_dict_key_list):
        param1 = para_dict_key_list[0]
        param2 = para_dict_key_list[1]
        param3 = para_dict_key_list[2]

        """
        sharpe_ratio_list = []

        len_of_para = len(para_dict[param3])

        for i in range(len_of_para):
            sharpe_ratio = result_df[(result_df[param1] == 30) & (result_df[param2] == 1)]["strat_sharpe"].values[i]
            print(sharpe_ratio)
            sharpe_ratio_list.append(sharpe_ratio)

        pivot_table = result_df.pivot(index = param1, columns = param2, values = "strat_sharpe")
        print(pivot_table)
        """

        self.create_sharpe_ratio_surface(result_df, param1, param2, param3, "SR")
        self.create_sharpe_ratio_graph(result_df, param1, param2, "SR for para1 and para2")
        self.create_sharpe_ratio_graph(result_df, param1, param3, "SR for para1 and para3")
        self.create_sharpe_ratio_graph(result_df, param2, param3, "SR for para2 and para3")

    def calculate_sharpe_stats(self, result_df, param1, param2):
        grouped = result_df.groupby([param1, param2])
        stats   = grouped["strat_sharpe"].describe()

        return stats

    def _get_para_dict(self):
        """
        para_dict = {
            "rolling_window" : [10, 20, 30], # rw cannot be 0
            "upper_band"     : [0, 1, 2, 3, 4],
            "lower_band"     : [0, 1, 2, 3, 4]
        }
        """
        para_dict = {
            "rolling_window" : [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            "upper_band"     : [0, 0.1, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4, 4.25, 4.5, 4.75, 5],
            "lower_band"     : [0, 0.1, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4, 4.25, 4.5, 4.75, 5]
        }

        return para_dict

    def _get_manager_list(self):
        manager_list = mp.Manager().list()

        return manager_list

    def _get_para_list(self, para_dict):
        para_list = list(para_dict.values())

        return para_list

    def _get_all_para_combination(self, para_list, df, manager_list):
        all_para_combination = list(itertools.product(*para_list))

        # Add df (backtesting object that is out of para_dict)0
        for i in range(len(all_para_combination)):
            para_combination = all_para_combination[i]
            para_combination = list(para_combination)

            para_combination.append(df)
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
    """
    def _get_return_list(self, all_para_combination):
        processes  = self.processes
        contractor = self.contractor

        pool        = mp.Pool(processes = processes)
        return_list = pool.map(contractor, all_para_combination)
        pool.close()

        return return_list
        """

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

    def contractor(self, para_combination):
        if len(para_combination) > 1:
            manager_list = para_combination[-1]
            manager_list.append(self.backtest(para_combination[0:-1]))

    def backtest(self, para_combination):
        asset       = self.asset
        strategy    = self.strategy
        exchange    = self.exchange
        instrument  = self.instrument
        product     = self.product
        interval    = self.interval
        symbol      = self.symbol

        binance_tx_fee_rate = self.binance_tx_fee_rate
        ann_multiple        = self.ann_multiple

        base_csv_existed, base_csv = self.check_base_csv(para_combination)

        if base_csv_existed == False:
            backtest_ready_list = self.get_backtest_ready_list_without_base_csv(para_combination)

        else:
            backtest_ready_list = self.get_backtest_ready_list_with_base_csv(base_csv, para_combination)

        single_result_list = self.get_single_backtest_result(backtest_ready_list, base_csv_existed, para_combination)

        signal_list = single_result_list[0]

        long_trading_pnl_list  = single_result_list[1]
        short_trading_pnl_list = single_result_list[2]

        long_fr_pnl_list  = single_result_list[3]
        short_fr_pnl_list = single_result_list[4]

        long_tx_fee_list  = single_result_list[5]
        short_tx_fee_list = single_result_list[6]

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
        strat_neg_pnl_day = [num for num in strat_full_pnl_list if num < -1 * binance_tx_fee_rate]

        long_pos_pnl_day = [num for num in long_full_pnl_list if num > 0]
        long_neg_pnl_day = [num for num in long_full_pnl_list if num < -1 * binance_tx_fee_rate]

        short_pos_pnl_day = [num for num in short_full_pnl_list if num > 0]
        short_neg_pnl_day = [num for num in short_full_pnl_list if num < -1 * binance_tx_fee_rate]

        strat_win_rate = self._calculate_win_rate(strat_pos_pnl_day, strat_neg_pnl_day)
        long_win_rate  = self._calculate_win_rate(long_pos_pnl_day, long_neg_pnl_day)
        short_win_rate = self._calculate_win_rate(short_pos_pnl_day, short_neg_pnl_day)

        strat_ann_return = np.around(np.mean(strat_full_pnl_list) * ann_multiple, decimals = 4)
        long_ann_return  = np.around(np.mean(long_full_pnl_list) * ann_multiple, decimals = 4)
        short_ann_return = np.around(np.mean(short_full_pnl_list) * ann_multiple, decimals = 4)

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

    def check_base_csv(self, para_combination):
        finished_path = self.finished_path

        symbol = self.symbol

        rolling_window = para_combination[0]
        upper_band     = para_combination[1]
        lower_band     = para_combination[2]

        base_csv_existed = False

        base_path = f"{finished_path}/{symbol}/single_result"
        base_csv  = f"{base_path}/{symbol}_{rolling_window}_{upper_band}_{lower_band}.csv"

        if os.path.isfile(base_csv) == True:
            base_csv_existed = True

        return base_csv_existed, base_csv

    def get_backtest_ready_list_without_base_csv(self, para_combination):
        df = self.df

        rolling_window = para_combination[0]

        df["ma"]      = df["basis_index_price_oi"].rolling(rolling_window).mean()
        df["sd"]      = df["basis_index_price_oi"].rolling(rolling_window).std()
        df["z_score"] = (df["basis_index_price_oi"] - df["ma"]) / df["sd"]
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

    def get_single_backtest_result(self, backtest_ready_list, base_csv_existed, para_combination):
        df = self.df

        asset       = self.asset
        strategy    = self.strategy
        exchange    = self.exchange
        instrument  = self.instrument
        product     = self.product
        interval    = self.interval
        symbol      = self.symbol

        binance_tx_fee_rate = self.binance_tx_fee_rate

        rolling_window = para_combination[0]
        upper_band     = para_combination[1]
        lower_band     = para_combination[2]

        open_price  = 0
        close_price = 0

        signal = 0

        long_trading_pnl  = 0
        short_trading_pnl = 0

        long_fr_pnl  = 0
        short_fr_pnl = 0

        long_tx_fee  = 0
        short_tx_fee = 0

        long_pos_opened  = backtest_ready_list[0]
        short_pos_opened = backtest_ready_list[1]

        signal_list = backtest_ready_list[2]

        long_trading_pnl_list  = backtest_ready_list[3]
        short_trading_pnl_list = backtest_ready_list[4]

        long_fr_pnl_list  = backtest_ready_list[5]
        short_fr_pnl_list = backtest_ready_list[6]

        long_tx_fee_list  = backtest_ready_list[7]
        short_tx_fee_list = backtest_ready_list[8]

        single_result_list = []

        loop_len = len(df) - len(signal_list)

        for i in range(loop_len):
            now_price   = df.loc[i, "open"]
            now_fr      = df.loc[i, "fundingRate"]
            now_z_score = df.loc[i, "z_score"]

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

                long_tx_fee = binance_tx_fee_rate
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

                short_tx_fee = binance_tx_fee_rate
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

                long_tx_fee = binance_tx_fee_rate
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

                short_tx_fee = binance_tx_fee_rate
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

                long_tx_fee = binance_tx_fee_rate
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

                short_tx_fee = binance_tx_fee_rate
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

        print(strategy, symbol, interval, product, instrument, exchange, asset, "action = finished backtest", "(", rolling_window, upper_band, lower_band, ")", )

        single_result_df = self._get_single_result_df(df, signal_list, long_trading_pnl_list, short_trading_pnl_list, long_fr_pnl_list, short_fr_pnl_list, long_tx_fee_list, short_tx_fee_list)
        self._store_single_result_df(single_result_df, rolling_window, upper_band, lower_band)

        single_result_list.append(signal_list)
        single_result_list.append(long_trading_pnl_list)
        single_result_list.append(short_trading_pnl_list)
        single_result_list.append(long_fr_pnl_list)
        single_result_list.append(short_fr_pnl_list)
        single_result_list.append(long_tx_fee_list)
        single_result_list.append(short_tx_fee_list)

        single_result_list.append(df)

        return single_result_list

    def _get_single_result_df(self, df, signal_list, long_trading_pnl_list, short_trading_pnl_list, long_fr_pnl_list, short_fr_pnl_list, long_tx_fee_list, short_tx_fee_list):
        signal_df = pd.DataFrame(signal_list)

        long_trading_pnl_df  = pd.DataFrame(long_trading_pnl_list)
        short_trading_pnl_df = pd.DataFrame(short_trading_pnl_list)

        long_fr_pnl_df  = pd.DataFrame(long_fr_pnl_list)
        short_fr_pnl_df = pd.DataFrame(short_fr_pnl_list)

        long_tx_fee_df  = pd.DataFrame(long_tx_fee_list)
        short_tx_fee_df = pd.DataFrame(short_tx_fee_list)

        df_col_list     = df.columns.tolist()
        append_col_list = ["signal",
                    "long_trading_pnl", "short_trading_pnl",
                    "long_fr_pnl", "short_fr_pnl",
                    "long_tx_fee", "short_tx_fee"]

        df_col_list.extend(append_col_list)

        single_result_df = pd.concat((df, signal_df,
                           long_trading_pnl_df, short_trading_pnl_df,
                           long_fr_pnl_df, short_fr_pnl_df,
                           long_tx_fee_df, short_tx_fee_df), axis = 1)

        single_result_df.columns = df_col_list

        return single_result_df

    def _store_single_result_df(self, single_result_df, rolling_window, upper_band, lower_band):
        finished_path = self.finished_path

        symbol = self.symbol

        result_path = f"{finished_path}/{symbol}"
        self._create_folder(result_path)

        single_result_path = f"{result_path}/single_result"
        self._create_folder(single_result_path)

        single_result_csv = f"{single_result_path}/{symbol}_{rolling_window}_{upper_band}_{lower_band}.csv"
        single_result_df.to_csv(single_result_csv, index = False)

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

    def create_sharpe_ratio_graph(self, result_df, x_param, y_param, title):
        x_values     = result_df[x_param]
        y_values     = result_df[y_param]
        sharpe_ratio = result_df["strat_sharpe"]

        fig = plt.figure()
        ax  = fig.add_subplot(111, projection = "3d")

        surface = ax.plot_trisurf(x_values, y_values, sharpe_ratio, cmap = "viridis")

        ax.set_xlabel(x_param)
        ax.set_ylabel(y_param)
        ax.set_zlabel("Sharpe Ratio")
        ax.set_title(title)
        plt.colorbar(surface)

        plt.show()

    def create_sharpe_ratio_surface(self, result_df, x_param, y_param, z_param, title):
        x_values     = result_df[x_param]
        y_values     = result_df[y_param]
        z_values     = result_df[z_param]
        sharpe_ratio = result_df["strat_sharpe"]

        fig = plt.figure()
        ax  = fig.add_subplot(111, projection = "3d")

        sc = ax.scatter(x_values, y_values, z_values, c = sharpe_ratio, cmap = "viridis")

        ax.set_xlabel(x_param)
        ax.set_ylabel(y_param)
        ax.set_zlabel(z_param)
        ax.set_title(title)
        plt.colorbar(sc, label = "Sharpe Ratio")

        plt.show()

class DataProcessor:
    def __init__(self, backtest_df_ready = False, symbol = None):
        self.asset      = "Cryptocurrency"
        self.strategy   = "testing_basis_index_price_open_interest"
        self.exchange   = "binance"
        self.instrument = "futures"
        self.product    = "coinm_futures"
        self.type       = "PERPETUAL"
        self.price_func = "kline"
        self.fr_func    = "funding_rate"
        self.basis_func = "basis"
        self.interval   = "8h"
        self.symbol     = symbol

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
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        interval   = self.interval

        base_folder = "D:\\backtest\\" + asset
        self._create_folder(base_folder)

        zero_folder = f"{base_folder}/{strategy}"
        self._create_folder(zero_folder)

        first_folder = f"{zero_folder}/{exchange}"
        self._create_folder(first_folder)

        second_folder = f"{first_folder}/{instrument}"
        self._create_folder(second_folder)

        third_folder = f"{second_folder}/{product}"
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
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        interval   = self.interval

        finished_path = f"D:/backtest/{asset}/{strategy}/{exchange}/{instrument}/{product}/{interval}"

        return finished_path

    def _get_price_df(self):
        symbol = self.symbol

        price_path = self._get_price_path()
        price_csv  = f"{price_path}/{symbol}.csv"

        price_df = pd.read_csv(price_csv)
        price_df = price_df[["open_time", "datetime", "open"]]
        price_df = price_df.round({"open_time": -3})
        price_df.rename(columns = {"open_time": "time"}, inplace = True)

        return price_df

    def _get_price_path(self):
        asset      = self.asset
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = self.price_func
        interval   = self.interval

        price_path = f"D:/{asset}/{exchange}/{instrument}/{product}/{function}/{interval}"

        return price_path

    def _get_funding_rate_df(self):
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = self.price_func
        interval   = self.interval
        symbol     = self.symbol

        funding_rate_path = self._get_funding_rate_path()
        funding_rate_csv  = f"{funding_rate_path}/{symbol}.csv"

        try:
            funding_rate_df = pd.read_csv(funding_rate_csv)
            funding_rate_df = funding_rate_df[["fundingTime", "datetime", "fundingRate"]]
            funding_rate_df = funding_rate_df.round({"fundingTime": -3})
            funding_rate_df.rename(columns = {"fundingTime": "time"}, inplace = True)

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "function": function, "product": product, "instrument": instrument, "exchange": exchange, "asset": asset,  "msg": "no funding_rate data file"}

            if symbol[-6:].isdigit() == True:
                pass

            else:
                message = str(message)
                self._send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, function, product, instrument, exchange, asset, "response = failed backtest, reason = without funding_rate data file")
            print("**************************************************")

            raise StopIteration

        return funding_rate_df

    def _get_funding_rate_path(self):
        asset      = self.asset
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = self.fr_func
        interval   = self.interval

        funding_rate_path = f"D:/{asset}/{exchange}/{instrument}/{product}/{function}/{interval}"

        return funding_rate_path

    def get_basis_df(self):
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = self.price_func
        interval   = self.interval
        type       = self.type
        symbol     = self.symbol

        splitted_part = symbol.split("_")
        basis_symbol  = splitted_part[0]

        basis_path = self.get_basis_path()
        basis_csv  = f"{basis_path}/{basis_symbol}.csv"

        try:
            basis_df = pd.read_csv(basis_csv)
            basis_df = basis_df[["timestamp", "indexPrice", "futuresPrice", "basis", "basisRate"]]
            basis_df["basis_index_price"] = basis_df["basis"] / basis_df["indexPrice"]
            basis_df.rename(columns = {"timestamp": "time"}, inplace = True)

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "product": product, "instrument": instrument, "exchange": exchange, "asset": asset, "msg": "no basis data file"}
            message = str(message)
            self._send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, product, instrument, exchange, asset, "response = failed backtest, reason = without basis data file")
            print("**************************************************")

            raise StopIteration

        return basis_df

    def get_basis_path(self):
        asset      = self.asset
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = self.basis_func
        interval   = "4h"
        type       = self.type

        basis_path = f"D:/{asset}/{exchange}/{instrument}/{product}/{function}/{interval}/{type}"

        return basis_path

    def get_open_interest_df(self):
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = self.price_func
        interval   = self.interval
        symbol     = self.symbol

        open_interest_path = self.get_open_interest_path()
        open_interest_csv  = f"{open_interest_path}/{symbol}.csv"

        try:
            open_interest_df = pd.read_csv(open_interest_csv)
            open_interest_df = open_interest_df[["time", "datetime", "open"]]
            open_interest_df = open_interest_df.round({"time": -3})
            open_interest_df.rename(columns = {"open": "openInterest"}, inplace = True)

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "product": product, "instrument": instrument, "exchange": exchange, "asset": asset, "msg": "no open_interest data file"}

            if symbol[-6:].isdigit() == True:
                pass

            else:
                message = str(message)
                self._send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, product, instrument, exchange, asset, "response = failed backtest, reason = without open_interest data file")
            print("**************************************************")

            raise StopIteration

        return open_interest_df

    def get_open_interest_path(self):
        asset      = self.asset
        source     = "coinglass"
        instrument = self.instrument
        function   = "oi_ohlc"
        type       = "perpetual"
        interval   = "h8"
        exchange   = "Binance"
        symbol     = self.symbol

        open_interest_path = f"D:/{asset}/{source}/{instrument}/{function}/{type}/{interval}/{exchange}"

        return open_interest_path

    def _get_price_fr_df(self):
        funding_rate_df = self._get_funding_rate_df()
        price_df        = self._get_price_df()

        price_fr_df = pd.merge(funding_rate_df, price_df, on = "time", how = "inner")
        price_fr_df.rename(columns = {"datetime_x": "datetime"}, inplace = True)
        price_fr_df = price_fr_df[["time", "datetime", "open", "fundingRate"]]

        return price_fr_df

    def get_price_fr_basis_df(self):
        basis_df    = self.get_basis_df()
        price_fr_df = self._get_price_fr_df()

        price_fr_basis_df = pd.merge(basis_df, price_fr_df, on = "time", how = "inner")
        price_fr_basis_df.rename(columns = {"datetime_x": "datetime"}, inplace = True)

        price_fr_basis_df = price_fr_basis_df[["time", "datetime", "open", "fundingRate", "basis_index_price"]]
        price_fr_basis_df = self._clean_data(price_fr_basis_df)

        return price_fr_basis_df

    def get_formatted_backtest_df(self):
        open_interest_df  = self.get_open_interest_df()
        price_fr_basis_df = self.get_price_fr_basis_df()

        backtest_df = pd.merge(open_interest_df, price_fr_basis_df, on = "time", how = "inner")
        backtest_df.rename(columns = {"datetime_x": "datetime"}, inplace=True)

        backtest_df["basis_index_price_oi"] = backtest_df["basis_index_price"] / backtest_df["openInterest"]

        backtest_df = backtest_df[["time", "datetime", "open", "fundingRate", "basis_index_price_oi"]]
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

    # symbol_list   = ["APEUSD_PERP"]
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