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

        self.full_para_backtest = True  # default = False
        self.delete_file        = False # default = False

        self.single_df_name = "backtest_set" # default = "backtest_set"
        self.symbol         = "BTCUSDT"      # default = "BTCUSDT"
        self.rolling_window = 10             # default = 10
        self.upper_band     = 1              # default = 1
        self.lower_band     = 1              # default = 1

        self.felixDataProcessor = FelixDataProcessor(self.strategy, self.category, self.interval)

    def _start_first_round_backtest(self):
        first_round_backtest = True
        full_para_backtest   = self.full_para_backtest

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

            else:
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
        full_para_backtest   = self.full_para_backtest

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

    def draw_full_backtest_result_curves(self):
        single_df_name = self.single_df_name
        symbol         = self.symbol
        rolling_window = self.rolling_window
        upper_band     = self.upper_band
        lower_band     = self.lower_band

        bybitCurveDrawer = BybitCurveDrawer(self.strategy, self.category, self.interval, symbol, single_df_name, rolling_window, upper_band, lower_band)
        bybitCurveDrawer._draw_curves()

def main():
    felix = Felix()
    # felix._start_first_round_backtest()
    # print("----------------------------------------------------------------------------------------------------")
    # felix._screen_full_backtest_result()
    # print("----------------------------------------------------------------------------------------------------")
    # felix.manage_full_backtest_result()
    # print("----------------------------------------------------------------------------------------------------")

    felix._start_second_round_backtest()
    print("----------------------------------------------------------------------------------------------------")

    # felix.draw_full_backtest_result_curves()
    # print("----------------------------------------------------------------------------------------------------")

if __name__ == "__main__":
    main()