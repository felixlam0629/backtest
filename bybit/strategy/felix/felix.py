from bybit.BybitBacktestSystem import BybitBacktestSystem
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

def main():
    felix = Felix()
    felix._start_backtest_engine()

if __name__ == "__main__":
    main()