import calendar
import datetime
import numpy as np
import os
import pandas as pd
from pprint import pprint
import requests

class BybitDataProcessor:
    def __init__(self, strategy = None, category = None, interval = None, symbol = None):
        self.asset    = "cryptocurrency"
        self.exchange = "bybit"
        self.strategy = strategy
        self.category = category
        self.interval = interval
        self.symbol   = symbol

        self.start_dt = datetime.datetime(2020, 1, 1, 0, 0, 0)
        # self.end_dt   = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self.end_dt   = datetime.datetime.now()

        self.seconds_to_ms = 1000
        self.start_int     = calendar.timegm(self.start_dt.timetuple()) * self.seconds_to_ms
        self.end_int       = calendar.timegm(self.end_dt.timetuple()) * self.seconds_to_ms

    def _get_base_backtest_df(self):
        funding_rate_path = self._get_funding_rate_path()
        funding_rate_df   = self._get_funding_rate_df(funding_rate_path)

        price_path = self._get_price_path()
        price_df   = self._get_price_df(price_path)

        price_fr_df = self._get_price_fr_df(funding_rate_df, price_df)

        open_interest_path = self._get_open_interest_path()
        open_interest_df   = self._get_open_interest_df(open_interest_path)

        base_backtest_df = self._get_formatted_base_backtest_df(open_interest_df, price_fr_df)

        return base_backtest_df

    def _get_price_path(self):
        price_function = "kline"
        price_interval = "240"

        price_path = f"D:/data/{self.asset}/{self.exchange}/{price_function}/{self.category}/{price_interval}"

        return price_path

    def _get_price_df(self, price_path):
        price_csv  = f"{price_path}/{self.symbol}.csv"

        price_df = pd.read_csv(price_csv)
        price_df = price_df[["time", "datetime", "open"]]
        price_df = price_df.round({"open_time": -3})
        price_df.rename(columns = {"open_time": "time"}, inplace = True)

        price_df = price_df[(price_df["time"] >= self.start_int) & (price_df["time"] <= self.end_int)]

        return price_df

    def _get_funding_rate_path(self):
        funding_rate_function = "funding_rate"
        funding_rate_interval = "480"

        funding_rate_path = f"D:/data/{self.asset}/{self.exchange}/{funding_rate_function}/{self.category}/{funding_rate_interval}"

        return funding_rate_path

    def _get_funding_rate_df(self, funding_rate_path):
        asset    = self.asset
        strategy = self.strategy
        exchange = self.exchange
        category = self.category
        function = "funding_rate"
        interval = self.interval
        symbol   = self.symbol

        funding_rate_csv = f"{funding_rate_path}/{symbol}.csv"

        try:
            funding_rate_df = pd.read_csv(funding_rate_csv)
            funding_rate_df = funding_rate_df[["time", "datetime", "funding_rate"]]
            funding_rate_df = funding_rate_df.round({"time": -3})

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "category": category, "function": function, "exchange": exchange, "asset": asset,  "msg": "no funding_rate data file"}

            if symbol[-6:].isdigit() == True:
                pass

            else:
                message = str(message)
                self.__send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, category, function, exchange, "response = failed backtest, reason = without funding_rate data file")
            print("**************************************************")

            raise StopIteration

        return funding_rate_df

    def _get_price_fr_df(self, funding_rate_df, price_df):
        price_fr_df = pd.merge(funding_rate_df, price_df, on = "time", how = "inner")
        price_fr_df.rename(columns = {"datetime_x": "datetime"}, inplace = True)
        price_fr_df = price_fr_df[["time", "datetime", "open", "funding_rate"]]

        return price_fr_df

    def _get_open_interest_path(self):
        open_interest_function = "open_interest"
        open_interest_interval = "4h"

        open_interest_path = f"D:/data/{self.asset}/{self.exchange}/{open_interest_function}/{self.category}/{open_interest_interval}"

        return open_interest_path

    def _get_open_interest_df(self, open_interest_path):
        asset    = self.asset
        strategy = self.strategy
        exchange = self.exchange
        category = self.category
        function = "open_interest"
        interval = "4h"
        symbol   = self.symbol

        open_interest_csv = f"{open_interest_path}/{symbol}.csv"

        try:
            open_interest_df = pd.read_csv(open_interest_csv)
            open_interest_df = open_interest_df[["time", "datetime", "open_interest"]]
            open_interest_df = open_interest_df.round({"time": -3})
            open_interest_df.rename(columns = {"open": "openInterest"}, inplace = True)

        except:
            message = {"strategy": strategy, "symbol": symbol, "interval": interval, "category": category, "function": function, "exchange": exchange, "asset": asset, "msg": "no open_interest data file"}

            if symbol[-6:].isdigit() == True:
                pass

            else:
                message = str(message)
                self.__send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, category, function, exchange, "response = failed backtest, reason = without open_interest data file")
            print("**************************************************")

            raise StopIteration

        return open_interest_df

    def _get_formatted_base_backtest_df(self, open_interest_df, price_fr_df):
        base_backtest_df = pd.merge(open_interest_df, price_fr_df, on = "time", how = "inner")
        base_backtest_df.rename(columns = {"datetime_x": "datetime"}, inplace=True)

        base_backtest_df = base_backtest_df[["time", "datetime", "open", "funding_rate", "open_interest"]]
        base_backtest_df = self.__clean_data(base_backtest_df)

        return base_backtest_df

    def _get_symbol_list(self):
        symbol_list_path = self._get_price_path()
        symbol_list      = self.__get_file_list(symbol_list_path)

        formatted_symbol_list = []

        for symbol in symbol_list:
            last_c = symbol[-1]

            if ("-" not in symbol) and (last_c.isdigit() == False):
                formatted_symbol_list.append(symbol)

        return formatted_symbol_list

    def _get_finished_path(self):
        finished_path = f"D:/backtest/{self.asset}/{self.exchange}/{self.strategy}/{self.category}/{self.interval}"

        return finished_path

    def _get_finished_list(self):
        finished_path = self._get_finished_path()

        try:
            finished_list = self.__get_file_list(finished_path)

            if "full_symbol_backtest_result" in finished_list:
                finished_list.remove("full_symbol_backtest_result")

        except FileNotFoundError:
            finished_list = []

            self.__create_folders()

        return finished_list

    def __clean_data(self, df):
        df.dropna(inplace = True)  # drop Nan value
        # df = df[df != 0].dropna()  # drop "0" value
        df = df.reset_index(drop = True)  # reset row index

        return df

    def __create_folder(self, folder):
        """ This function is used to create the folder from the path.

        Args:
            folder (str): path of the file
        """

        if os.path.isdir(folder) == False:
            os.mkdir(folder)

    def __create_folders(self):
        base_folder = "D:\\backtest\\" + self.asset
        self.__create_folder(base_folder)

        zero_folder = f"{base_folder}/{self.exchange}"
        self.__create_folder(zero_folder)

        first_folder = f"{zero_folder}/{self.strategy}"
        self.__create_folder(first_folder)

        second_folder = f"{first_folder}/{self.category}"
        self.__create_folder(second_folder)

        third_folder = f"{second_folder}/{self.interval}"
        self.__create_folder(third_folder)

    def __get_file_list(self, path):
        file_list = []

        for file in os.listdir(path):
            if os.path.isfile(os.path.join(path, file)):
                file = os.path.splitext(file)[0]
                file_list.append(file)

            else:
                file_list.append(file)

        return file_list

    def __send_tg_msg_to_backtest_channel(self, message):
        base_url = "https://api.telegram.org/bot6233469935:AAHayu1tVZ4NleqRFM-61F6VQObWMCwF90U/sendMessage?chat_id=-809813823&text="
        requests.get(base_url + message)