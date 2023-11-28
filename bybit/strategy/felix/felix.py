"""
for tmr/ forever backtest update system
"""
import pandas as pd

from bybit.BybitBacktestSystem import BybitBacktestSystem
from bybit.BybitResultScreener import BybitResultScreener
from bybit.BybitResultManager import BybitResultManager
from bybit.BybitCurveDrawer import BybitCurveDrawer

from bybit.strategy.felix.FelixDataProcessor import FelixDataProcessor

class Felix:
    def __init__(self):
        self.strategy = "felix"
        self.category = "linear"
        self.interval = "480"

        self.delete_file = False # default = False

        self.felixDataProcessor = FelixDataProcessor(self.strategy, self.category, self.interval)

        # self.symbol         = "BTCUSDT" # default = "BTCUSDT"
        # self.rolling_window = 20        # default = 10
        # self.upper_band     = 2.5       # default = 1
        # self.lower_band     = 1.75      # default = 1

    def _start_test_round_backtest(self):
        first_round_backtest = True
        full_para_backtest   = False

        symbol_list   = ["BTCUSDT"]
        finished_list = self.felixDataProcessor._get_finished_list()

        for symbol in symbol_list:
            if symbol not in finished_list:
                try:
                    felixDataProcessor         = FelixDataProcessor(self.strategy, self.category, self.interval, symbol)
                    backtest_df, finished_path = felixDataProcessor._get_backtest_df_for_backtest_system()

                    bybitBacktestSystem = BybitBacktestSystem(self.strategy, self.category, self.interval, symbol,
                                                              finished_path, backtest_df,
                                                              first_round_backtest, full_para_backtest)
                    bybitBacktestSystem._run_full_backtest_system()

                except StopIteration:
                    pass
    def _start_first_round_backtest(self):
        first_round_backtest = True
        full_para_backtest   = True

        symbol_list   = self.felixDataProcessor._get_symbol_list()
        finished_list = self.felixDataProcessor._get_finished_list()

        for symbol in symbol_list:
            if symbol not in finished_list:
                try:
                    felixDataProcessor         = FelixDataProcessor(self.strategy, self.category, self.interval, symbol)
                    backtest_df, finished_path = felixDataProcessor._get_backtest_df_for_backtest_system()

                    bybitBacktestSystem = BybitBacktestSystem(self.strategy, self.category, self.interval, symbol,
                                                              finished_path, backtest_df,
                                                              first_round_backtest, full_para_backtest)
                    bybitBacktestSystem._run_full_backtest_system()

                except StopIteration:
                    pass

    def _screen_full_backtest_result(self):
        bybitResultScreener = BybitResultScreener(self.strategy, self.category, self.interval)
        bybitResultScreener._generate_full_symbol_backtest_report()

    def manage_full_backtest_result(self):
        delete_file = self.delete_file

        bybitResultManager = BybitResultManager(self.strategy, self.category, self.interval, delete_file)
        bybitResultManager._delete_useless_backtest_result()

    def _start_second_round_backtest(self):
        first_round_backtest = False
        full_para_backtest   = True

        finished_list = self.felixDataProcessor._get_finished_list()

        for symbol in finished_list:
            try:
                felixDataProcessor         = FelixDataProcessor(self.strategy, self.category, self.interval, symbol)
                backtest_df, finished_path = felixDataProcessor._get_backtest_df_for_backtest_system()

                bybitBacktestSystem = BybitBacktestSystem(self.strategy, self.category, self.interval, symbol,
                                                          finished_path, backtest_df,
                                                          first_round_backtest, full_para_backtest)
                bybitBacktestSystem._run_full_backtest_system()

            except StopIteration:
                pass

    def _draw_full_backtest_result_curves(self):
        finished_path       = self.felixDataProcessor.get_finished_path()
        finished_list       = self.felixDataProcessor._get_finished_list()
        single_df_name_list = ["backtest_set", "training_set", "testing_set"]

        for symbol in finished_list:
            for single_df_name in single_df_name_list:
                full_result_path = f"{finished_path}/{symbol}/{single_df_name}/full_result"
                full_result_csv  = f"{full_result_path}/{symbol}.csv"
                full_result_df   = pd.read_csv(full_result_csv)

                rolling_window = int(full_result_df.loc[0, "rolling_window"])
                upper_band     = int(full_result_df.loc[0, "upper_band"])
                lower_band     = int(full_result_df.loc[0, "lower_band"])

                bybitCurveDrawer = BybitCurveDrawer(self.strategy, self.category, self.interval, symbol, single_df_name, rolling_window, upper_band, lower_band)
                bybitCurveDrawer._draw_curves()

def main():
    felix = Felix()
    # felix._start_test_round_backtest()
    # print("----------------------------------------------------------------------------------------------------")
    # felix._start_first_round_backtest()
    # print("----------------------------------------------------------------------------------------------------")
    # felix._screen_full_backtest_result()
    # print("----------------------------------------------------------------------------------------------------")
    # felix.manage_full_backtest_result()
    # print("----------------------------------------------------------------------------------------------------")
    # felix._start_second_round_backtest()
    # print("----------------------------------------------------------------------------------------------------")
    # felix._draw_full_backtest_result_curves()
    # print("----------------------------------------------------------------------------------------------------")

if __name__ == "__main__":
    main()