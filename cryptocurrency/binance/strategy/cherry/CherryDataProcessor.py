import calendar
import datetime
import pandas as pd
import requests

from cryptocurrency.binance.strategy.BinanceDataProcessor import BinanceDataProcessor

class CherryDataProcessor:
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

        top_trader_ls_ratio_pos_path = self._get_top_trader_ls_ratio_pos_path()
        top_trader_ls_ratio_pos_df   = self._get_top_trader_ls_ratio_pos_df(top_trader_ls_ratio_pos_path)

        backtest_df = self.get_formatted_backtest_df(top_trader_ls_ratio_pos_df, base_backtest_df)

        return backtest_df, finished_path

    def _get_top_trader_ls_ratio_pos_path(self): # from coinglass
        top_trader_ls_ratio_pos_source   = "coinglass"
        top_trader_ls_ratio_pos_function = "top_ls_pos"
        top_trader_ls_ratio_pos_type     = "perpetual"
        top_trader_ls_ratio_pos_interval = "h8"

        top_trader_ls_ratio_pos_path = f"D:/data/{self.asset}/{top_trader_ls_ratio_pos_source}/{self.instrument}/{top_trader_ls_ratio_pos_function}/{top_trader_ls_ratio_pos_type}/{top_trader_ls_ratio_pos_interval}/{self.exchange}"

        return top_trader_ls_ratio_pos_path
    def _get_top_trader_ls_ratio_pos_df(self, top_trader_ls_ratio_pos_path): # from coinglass
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = "top_ls_pos"
        interval   = self.interval
        symbol     = self.symbol

        top_trader_ls_ratio_pos_csv = f"{top_trader_ls_ratio_pos_path}/{symbol}.csv"

        try:
            top_trader_ls_ratio_pos_df = pd.read_csv(top_trader_ls_ratio_pos_csv)
            top_trader_ls_ratio_pos_df = top_trader_ls_ratio_pos_df[["createTime", "datetime", "longShortRatio"]]
            top_trader_ls_ratio_pos_df = top_trader_ls_ratio_pos_df.round({"createTime": -3})
            top_trader_ls_ratio_pos_df.rename(columns = {"createTime": "time"}, inplace=True)

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "product": product, "instrument": instrument, "function": function, "exchange": exchange, "asset": asset,
                       "msg": "no top_trader_ls_ratio_pos data file"}

            if symbol[-6:].isdigit() == True:
                pass

            else:
                message = str(message)
                self.__send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, product, instrument, function, exchange, "response = failed backtest, reason = without top_trader_ls_ratio_pos data file")
            print("**************************************************")

            raise StopIteration

        return top_trader_ls_ratio_pos_df

    """
    def _get_top_trader_ls_ratio_pos_path(self): # from binance as they do not have enough data yet
        top_trader_ls_ratio_pos_function = "top_trader_ls_ratio_pos"
        top_trader_ls_ratio_pos_interval = "4h"

        top_trader_ls_ratio_pos_path = f"D:/data/{self.asset}/{self.exchange}/{self.instrument}/{self.product}/{top_trader_ls_ratio_pos_function}/{top_trader_ls_ratio_pos_interval}"

        return top_trader_ls_ratio_pos_path
        
    def _get_top_trader_ls_ratio_pos_df(self, top_trader_ls_ratio_pos_path): # from binance as they do not have enough data yet
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        function   = "top_trader_ls_ratio_pos"
        interval   = "4h"
        symbol     = self.symbol

        top_trader_ls_ratio_pos_csv = f"{top_trader_ls_ratio_pos_path}/{symbol}.csv"

        try:
            top_trader_ls_ratio_pos_df = pd.read_csv(top_trader_ls_ratio_pos_csv)
            top_trader_ls_ratio_pos_df = top_trader_ls_ratio_pos_df[["timestamp", "datetime", "longShortRatio"]]
            top_trader_ls_ratio_pos_df = top_trader_ls_ratio_pos_df.round({"timestamp": -3})
            top_trader_ls_ratio_pos_df.rename(columns = {"timestamp": "time"}, inplace=True)

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "product": product, "instrument": instrument, "function": function, "exchange": exchange, "asset": asset,
                       "msg": "no top_trader_ls_ratio_pos data file"}

            if symbol[-6:].isdigit() == True:
                pass

            else:
                message = str(message)
                self.__send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, product, instrument, function, exchange, "response = failed backtest, reason = without top_trader_ls_ratio_pos data file")
            print("**************************************************")

            raise StopIteration

        return top_trader_ls_ratio_pos_df
        """

    def get_formatted_backtest_df(self, top_trader_ls_ratio_pos_df, base_backtest_df):
        base_backtest_df = pd.merge(top_trader_ls_ratio_pos_df, base_backtest_df, on = "time", how = "inner")
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