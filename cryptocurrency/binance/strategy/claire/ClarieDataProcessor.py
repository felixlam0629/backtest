import calendar
import datetime

from cryptocurrency.binance.strategy.BinanceDataProcessor import BinanceDataProcessor

class ClaireDataProcessor:
    def __init__(self, strategy = None, instrument = None, product = None, interval = None, symbol = None):
        self.asset      = "cryptocurrency"
        self.exchange   = "binance"
        self.strategy   = strategy
        self.instrument = instrument
        self.product    = product
        self.interval   = interval
        self.symbol     = symbol

        self.start_dt = datetime.datetime(2020, 1, 1, 0, 0, 0)
        # self.end_dt   = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self.end_dt   = datetime.datetime.now()

        self.seconds_to_ms = 1000
        self.start_int     = calendar.timegm(self.start_dt.timetuple()) * self.seconds_to_ms
        self.end_int       = calendar.timegm(self.end_dt.timetuple()) * self.seconds_to_ms

        self.binanceDataProcessor = BinanceDataProcessor(self.strategy, self.instrument, self.product, self.interval, self.symbol)

    def _get_backtest_df_for_backtest_system(self):
        base_backtest_df = self.binanceDataProcessor._get_base_backtest_df()
        finished_path    = self.binanceDataProcessor._get_finished_path()

        backtest_df = self.get_formatted_backtest_df(base_backtest_df)

        return backtest_df, finished_path

    def get_formatted_backtest_df(self, base_backtest_df):
        # add open_interest here
        base_backtest_df["backtest_data"] = base_backtest_df["fundingRate"]
        base_backtest_df                  = base_backtest_df[["time", "datetime", "open", "fundingRate", "backtest_data"]]

        backtest_df = self.__clean_data(base_backtest_df)

        return backtest_df

    def __clean_data(self, df):
        df.dropna(inplace = True)  # drop Nan value
        # df = df[df != 0].dropna()  # drop "0" value
        df = df.reset_index(drop = True)  # reset row index

        return df