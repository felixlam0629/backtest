import calendar
import datetime
import pandas as pd
import requests

from cryptocurrency.binance.strategy.BinanceDataProcessor import BinanceDataProcessor

class JadeDataProcessor:
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

        ls_ratio_path = self._get_ls_ratio_path()
        ls_ratio_df   = self._get_ls_ratio_df(ls_ratio_path)

        backtest_df = self.get_formatted_backtest_df(ls_ratio_df, base_backtest_df)

        return backtest_df, finished_path

    def _get_ls_ratio_path(self): # from coinglass
        ls_ratio_source   = "coinglass"
        ls_ratio_function = "ls_acc"
        ls_ratio_type     = "perpetual"
        ls_ratio_interval = "h8"

        ls_ratio_path = f"D:/data/{self.asset}/{ls_ratio_source}/{self.instrument}/{ls_ratio_function}/{ls_ratio_type}/{ls_ratio_interval}/{self.exchange}"

        return ls_ratio_path
    def _get_ls_ratio_df(self, ls_ratio_path): # from coinglass
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = "ls_ratio"
        interval   = self.interval
        symbol     = self.symbol

        ls_ratio_csv = f"{ls_ratio_path}/{symbol}.csv"

        try:
            ls_ratio_df = pd.read_csv(ls_ratio_csv)
            ls_ratio_df = ls_ratio_df[["createTime", "datetime", "longShortRatio"]]
            ls_ratio_df = ls_ratio_df.round({"createTime": -3})
            ls_ratio_df.rename(columns = {"createTime": "time"}, inplace=True)

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "product": product, "instrument": instrument, "function": function, "exchange": exchange, "asset": asset,
                       "msg": "no ls_ratio data file"}

            if symbol[-6:].isdigit() == True:
                pass

            else:
                message = str(message)
                self.__send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, product, instrument, function, exchange, "response = failed backtest, reason = without ls_ratio data file")
            print("**************************************************")

            raise StopIteration

        return ls_ratio_df

    """
    def _get_ls_ratio_path(self): # from binance as they do not have enough data yet
        ls_ratio_function = "ls_ratio"
        ls_ratio_interval = "4h"

        ls_ratio_path = f"D:/data/{self.asset}/{self.exchange}/{self.instrument}/{self.product}/{ls_ratio_function}/{ls_ratio_interval}"

        return ls_ratio_path
        
    def _get_ls_ratio_df(self, ls_ratio_path): # from binance as they do not have enough data yet
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = "ls_ratio"
        interval   = "4h"
        symbol     = self.symbol

        ls_ratio_csv = f"{ls_ratio_path}/{symbol}.csv"

        try:
            ls_ratio_df = pd.read_csv(ls_ratio_csv)
            ls_ratio_df = ls_ratio_df[["timestamp", "datetime", "longShortRatio"]]
            ls_ratio_df = ls_ratio_df.round({"timestamp": -3})
            ls_ratio_df.rename(columns = {"timestamp": "time"}, inplace=True)

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "product": product, "instrument": instrument, "function": function, "exchange": exchange, "asset": asset,
                       "msg": "no ls_ratio data file"}

            if symbol[-6:].isdigit() == True:
                pass

            else:
                message = str(message)
                self.__send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, product, instrument, function, exchange, "response = failed backtest, reason = without ls_ratio data file")
            print("**************************************************")

            raise StopIteration

        return ls_ratio_df
        """

    def get_formatted_backtest_df(self, ls_ratio_df, base_backtest_df):
        base_backtest_df = pd.merge(ls_ratio_df, base_backtest_df, on = "time", how = "inner")
        base_backtest_df.rename(columns = {"datetime_x": "datetime"}, inplace = True)
        base_backtest_df = base_backtest_df[["time", "datetime", "open", "fundingRate", "longShortRatio"]]

        base_backtest_df["backtest_data"] = base_backtest_df["longShortRatio"]
        base_backtest_df                  = base_backtest_df[["time", "datetime", "open", "fundingRate", "backtest_data"]]

        backtest_df = self.__clean_data(base_backtest_df)

        return backtest_df

    def __clean_data(self, df):
        df.dropna(inplace = True)  # drop Nan value
        # df = df[df != 0].dropna()  # drop "0" value
        df = df.reset_index(drop = True)  # reset row index

        return df

    def __send_tg_msg_to_backtest_channel(self, message):
        base_url = "https://api.telegram.org/bot6233469935:AAHayu1tVZ4NleqRFM-61F6VQObWMCwF90U/sendMessage?chat_id=-809813823&text="
        requests.get(base_url + message)