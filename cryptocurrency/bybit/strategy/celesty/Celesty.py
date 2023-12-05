import pandas as pd

from cryptocurrency.bybit.strategy.BybitDataProcessor import BybitDataProcessor
from cryptocurrency.bybit.strategy.BybitBacktestSystem import BybitBacktestSystem
from cryptocurrency.bybit.strategy.BybitResultScreener import BybitResultScreener
from cryptocurrency.bybit.strategy.BybitResultManager import BybitResultManager
from cryptocurrency.bybit.strategy.BybitCurveDrawer import BybitCurveDrawer

from cryptocurrency.bybit.strategy.celesty.CelestyDataProcessor import CelestyDataProcessor

class Celesty:
    def __init__(self):
        self.strategy = "celesty"
        self.category = "linear"
        self.interval = "480"

        self.delete_file = True # default = False

        self.bybitDataProcessor   = BybitDataProcessor(self.strategy, self.category, self.interval)
        self.celestyDataProcessor = CelestyDataProcessor(self.strategy, self.category, self.interval)

    def _start_test_round_backtest(self):
        first_round_backtest = True
        full_para_backtest   = False

        symbol_list   = ["BTCUSDT"]
        finished_list = self.bybitDataProcessor._get_finished_list()

        for symbol in symbol_list:
            if symbol not in finished_list:
                try:
                    celestyDataProcessor       = CelestyDataProcessor(self.strategy, self.category, self.interval, symbol)
                    backtest_df, finished_path = celestyDataProcessor._get_backtest_df_for_backtest_system()

                    bybitBacktestSystem = BybitBacktestSystem(self.strategy, self.category, self.interval, symbol,
                                                              finished_path, backtest_df,
                                                              first_round_backtest, full_para_backtest)
                    bybitBacktestSystem._run_full_backtest_system()

                except StopIteration:
                    pass

    def _start_first_round_backtest(self):
        first_round_backtest = True
        full_para_backtest   = True

        symbol_list   = self.bybitDataProcessor._get_symbol_list()
        finished_list = self.bybitDataProcessor._get_finished_list()

        for symbol in symbol_list:
            if symbol not in finished_list:
                try:
                    celestyDataProcessor       = CelestyDataProcessor(self.strategy, self.category, self.interval, symbol)
                    backtest_df, finished_path = celestyDataProcessor._get_backtest_df_for_backtest_system()

                    bybitBacktestSystem = BybitBacktestSystem(self.strategy, self.category, self.interval, symbol,
                                                              finished_path, backtest_df,
                                                              first_round_backtest, full_para_backtest)
                    bybitBacktestSystem._run_full_backtest_system()

                except StopIteration:
                    pass

    def _screen_full_backtest_result(self):
        bybitResultScreener = BybitResultScreener(self.strategy, self.category, self.interval)
        bybitResultScreener._generate_full_symbol_backtest_report()

    def _manage_full_backtest_result(self):
        delete_file = self.delete_file

        bybitResultManager = BybitResultManager(self.strategy, self.category, self.interval, delete_file)
        bybitResultManager._delete_useless_backtest_result()

    def _start_second_round_backtest(self):
        first_round_backtest = False
        full_para_backtest   = True

        finished_list = self.bybitDataProcessor._get_finished_list()

        for symbol in finished_list:
            try:
                celestyDataProcessor       = CelestyDataProcessor(self.strategy, self.category, self.interval, symbol)
                backtest_df, finished_path = celestyDataProcessor._get_backtest_df_for_backtest_system()

                bybitBacktestSystem = BybitBacktestSystem(self.strategy, self.category, self.interval, symbol,
                                                          finished_path, backtest_df,
                                                          first_round_backtest, full_para_backtest)
                bybitBacktestSystem._run_full_backtest_system()

            except StopIteration:
                pass

    def _draw_full_backtest_result_curves(self):
        finished_path       = self.bybitDataProcessor._get_finished_path()
        finished_list       = self.bybitDataProcessor._get_finished_list()
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
    celesty = Celesty()

    """
    # 1st phrase
    celesty._start_test_round_backtest()
    print("----------------------------------------------------------------------------------------------------")
    celesty._start_first_round_backtest()
    print("----------------------------------------------------------------------------------------------------")
    celesty._screen_full_backtest_result()
    print("----------------------------------------------------------------------------------------------------")
    """

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