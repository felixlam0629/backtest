import pandas as pd

from Binance.usdm_futures.strategy.BinanceDataProcessor import BinanceDataProcessor
from Binance.usdm_futures.strategy.BinanceBacktestSystem import BinanceBacktestSystem
from Binance.usdm_futures.strategy.BinanceResultScreener import BinanceResultScreener
from Binance.usdm_futures.strategy.BinanceResultManager import BinanceResultManager
from Binance.usdm_futures.strategy.BinanceCurveDrawer import BinanceCurveDrawer

from Binance.usdm_futures.strategy.celesty.CelestyDataProcessor import CelestyDataProcessor

class Celesty:
    def __init__(self):
        self.strategy   = "celesty"
        self.instrument = "futures"
        self.product    = "usdm_futures"
        self.interval   = "480"
        
        self.delete_file = True # default = False

        self.binanceDataProcessor = BinanceDataProcessor(self.strategy, self.category, self.interval)
        self.celestyDataProcessor = CelestyDataProcessor(self.strategy, self.category, self.interval)

        # self.symbol         = "BTCUSDT" # default = "BTCUSDT"
        # self.rolling_window = 20        # default = 10
        # self.upper_band     = 2.5       # default = 1
        # self.lower_band     = 1.75      # default = 1

    def _start_test_round_backtest(self):
        first_round_backtest = True
        full_para_backtest   = False

        symbol_list   = ["BTCUSDT"]
        finished_list = self.binanceDataProcessor._get_finished_list()

        for symbol in symbol_list:
            if symbol not in finished_list:
                try:
                    celestyDataProcessor       = CelestyDataProcessor(self.strategy, self.category, self.interval, symbol)
                    backtest_df, finished_path = celestyDataProcessor._get_backtest_df_for_backtest_system()

                    binanceBacktestSystem = BinanceBacktestSystem(self.strategy, self.category, self.interval, symbol,
                                                              finished_path, backtest_df,
                                                              first_round_backtest, full_para_backtest)
                    binanceBacktestSystem._run_full_backtest_system()

                except StopIteration:
                    pass

    def _start_first_round_backtest(self):
        first_round_backtest = True
        full_para_backtest   = True

        symbol_list   = self.BinanceDataProcessor._get_symbol_list()
        finished_list = self.BinanceDataProcessor._get_finished_list()

        for symbol in symbol_list:
            if symbol not in finished_list:
                try:
                    celestyDataProcessor       = CelestyDataProcessor(self.strategy, self.category, self.interval, symbol)
                    backtest_df, finished_path = celestyDataProcessor._get_backtest_df_for_backtest_system()

                    binanceBacktestSystem = BinanceBacktestSystem(self.strategy, self.category, self.interval, symbol,
                                                              finished_path, backtest_df,
                                                              first_round_backtest, full_para_backtest)
                    binanceBacktestSystem._run_full_backtest_system()

                except StopIteration:
                    pass

    def _screen_full_backtest_result(self):
        binanceResultScreener = BinanceResultScreener(self.strategy, self.category, self.interval)
        binanceResultScreener._generate_full_symbol_backtest_report()

    def _manage_full_backtest_result(self):
        delete_file = self.delete_file

        binanceResultManager = BinanceResultManager(self.strategy, self.category, self.interval, delete_file)
        binanceResultManager._delete_useless_backtest_result()

    def _start_second_round_backtest(self):
        first_round_backtest = False
        full_para_backtest   = True

        finished_list = self.BinanceDataProcessor._get_finished_list()

        for symbol in finished_list:
            try:
                celestyDataProcessor       = CelestyDataProcessor(self.strategy, self.category, self.interval, symbol)
                backtest_df, finished_path = celestyDataProcessor._get_backtest_df_for_backtest_system()

                binanceBacktestSystem = BinanceBacktestSystem(self.strategy, self.category, self.interval, symbol,
                                                          finished_path, backtest_df,
                                                          first_round_backtest, full_para_backtest)
                binanceBacktestSystem._run_full_backtest_system()

            except StopIteration:
                pass

    def _draw_full_backtest_result_curves(self):
        finished_path       = self.binanceDataProcessor._get_finished_path()
        finished_list       = self.binanceDataProcessor._get_finished_list()
        single_df_name_list = ["backtest_set", "training_set", "testing_set"]

        for symbol in finished_list:
            for single_df_name in single_df_name_list:
                full_result_path = f"{finished_path}/{symbol}/{single_df_name}/full_result"
                full_result_csv  = f"{full_result_path}/{symbol}.csv"
                full_result_df   = pd.read_csv(full_result_csv)

                rolling_window = int(full_result_df.loc[0, "rolling_window"])
                upper_band     = int(full_result_df.loc[0, "upper_band"])
                lower_band     = int(full_result_df.loc[0, "lower_band"])

                binanceCurveDrawer = BinanceCurveDrawer(self.strategy, self.category, self.interval, symbol, single_df_name, rolling_window, upper_band, lower_band)
                binanceCurveDrawer._draw_curves()

def main():
    celesty = Celesty()

    # 1st phrase
    celesty._start_test_round_backtest()
    print("----------------------------------------------------------------------------------------------------")
    celesty._start_first_round_backtest()
    print("----------------------------------------------------------------------------------------------------")
    celesty._screen_full_backtest_result()
    print("----------------------------------------------------------------------------------------------------")

    """
    # 2nd phrase
    celesty._manage_full_backtest_result()
    print("----------------------------------------------------------------------------------------------------")
    """

    """
    # final phrase
    celesty._start_second_round_backtest()
    print("----------------------------------------------------------------------------------------------------")
    celesty._draw_full_backtest_result_curves()
    print("----------------------------------------------------------------------------------------------------")
    """

if __name__ == "__main__":
    main()