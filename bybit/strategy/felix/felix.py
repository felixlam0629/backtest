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

        self.felixDataProcessor = FelixDataProcessor(self.strategy, self.category, self.interval)

    def _start_backtest_engine(self):
        symbol_list   = self.felixDataProcessor._get_symbol_list()
        finished_list = self.felixDataProcessor._get_finished_list()

        symbol_list = ["BTCPERP", "BTCUSDT"]
        # finished_list = []

        for symbol in symbol_list:
            if symbol not in finished_list:
                backtest_df_ready = False

            else:
                backtest_df_ready = True

            try:
                felixDataProcessor         = FelixDataProcessor(self.strategy, self.category, self.interval, symbol, backtest_df_ready)
                backtest_df, finished_path = felixDataProcessor._put_data_into_backtest_system()

                bybitBacktestSystem = BybitBacktestSystem(self.strategy, self.category, self.interval, symbol, finished_path, backtest_df)
                bybitBacktestSystem._run_full_backtest_system()

            except StopIteration:
                pass

    def screen_full_backtest_result(self):
        bybitResultScreener = BybitResultScreener(self.strategy, self.category, self.interval)
        bybitResultScreener._generate_full_symbol_backtest_report()

    def manage_full_backtest_result(self):
        bybitResultManager = BybitResultManager(self.strategy, self.category, self.interval)
        bybitResultManager._delete_useless_backtest_result()

    def draw_full_backtest_result_curves(self):
        symbol         = "BTCUSDT"
        rolling_window = 10
        upper_band     = 1
        lower_band     = 1

        bybitCurveDrawer = BybitCurveDrawer(self.strategy, self.category, self.interval, symbol, rolling_window, upper_band, lower_band)
        bybitCurveDrawer._draw_curves()

def main():
    felix = Felix()
    felix._start_backtest_engine()
    # felix.screen_full_backtest_result()
    # felix.manage_full_backtest_result()
    # felix.draw_full_backtest_result_curves()

if __name__ == "__main__":
    main()