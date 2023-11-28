import calendar
import datetime
import numpy as np
import os
import pandas as pd
from pprint import pprint
import requests

class FelixDataProcessor:
    def __init__(self, strategy = None, category = None, interval = None, symbol = None):
        self.asset    = "cryptocurrency"
        self.exchange = "bybit"
        self.strategy = strategy
        self.category = category
        self.interval = interval
        self.symbol   = symbol

        self.price_func  = "kline"
        self.fr_func     = "funding_rate"
        self.oi_func     = "open_interest"
        self.fr_interval = "240"

        self.start_dt = datetime.datetime(2020, 1, 1, 0, 0, 0)
        # self.end_dt   = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self.end_dt   = datetime.datetime.now()

        self.seconds_to_ms = 1000
        self.start_int     = calendar.timegm(self.start_dt.timetuple()) * self.seconds_to_ms
        self.end_int       = calendar.timegm(self.end_dt.timetuple()) * self.seconds_to_ms

    def _get_backtest_df_for_backtest_system(self):
        backtest_df   = self.get_formatted_backtest_df()
        finished_path = self.get_finished_path()

        return backtest_df, finished_path

    def _create_folder(self, folder):
        """ This function is used to create the folder from the path.

        Args:
            folder (str): path of the file
        """

        if os.path.isdir(folder) == False:
            os.mkdir(folder)

    def _create_folders(self):
        base_folder = "D:\\backtest\\" + self.asset
        self._create_folder(base_folder)

        zero_folder = f"{base_folder}/{self.exchange}"
        self._create_folder(zero_folder)

        first_folder = f"{zero_folder}/{self.strategy}"
        self._create_folder(first_folder)

        second_folder = f"{first_folder}/{self.category}"
        self._create_folder(second_folder)

        third_folder = f"{second_folder}/{self.interval}"
        self._create_folder(third_folder)

    def _get_symbol_list(self):
        symbol_list_path = self._get_price_path()
        symbol_list      = self._get_file_list(symbol_list_path)

        formatted_symbol_list = []

        for symbol in symbol_list:
            last_c = symbol[-1]

            if ("-" not in symbol) and (last_c.isdigit() == False):
                formatted_symbol_list.append(symbol)

        return formatted_symbol_list

    def _get_finished_list(self):
        finished_path = self.get_finished_path()

        try:
            finished_list = self._get_file_list(finished_path)

            if "full_symbol_backtest_result" in finished_list:
                finished_list.remove("full_symbol_backtest_result")

        except FileNotFoundError:
            finished_list = []

            self._create_folders()

        return finished_list

    def get_finished_path(self):
        finished_path = f"D:/backtest/{self.asset}/{self.exchange}/{self.strategy}/{self.category}/{self.interval}"

        return finished_path

    def _get_price_df(self):
        price_path = self._get_price_path()
        price_csv  = f"{price_path}/{self.symbol}.csv"

        price_df = pd.read_csv(price_csv)
        price_df = price_df[["time", "datetime", "open"]]
        price_df = price_df.round({"open_time": -3})
        price_df.rename(columns = {"open_time": "time"}, inplace = True)

        price_df = price_df[(price_df["time"] >= self.start_int) & (price_df["time"] <= self.end_int)]

        return price_df

    def _get_price_path(self):
        price_path = f"D:/data/{self.asset}/{self.exchange}/{self.price_func}/{self.category}/{self.fr_interval}"

        return price_path

    def _get_funding_rate_df(self):
        asset    = self.asset
        strategy = self.strategy
        exchange = self.exchange
        function = self.fr_func
        category = self.category
        interval = self.interval
        symbol   = self.symbol

        funding_rate_path = self._get_funding_rate_path()
        funding_rate_csv  = f"{funding_rate_path}/{symbol}.csv"

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
                self._send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, category, function, exchange, "response = failed backtest, reason = without funding_rate data file")
            print("**************************************************")

            raise StopIteration

        return funding_rate_df

    def _get_funding_rate_path(self):
        interval = "480"

        funding_rate_path = f"D:/data/{self.asset}/{self.exchange}/{self.fr_func}/{self.category}/{interval}"

        return funding_rate_path

    def get_open_interest_df(self):
        asset    = self.asset
        strategy = self.strategy
        exchange = self.exchange
        function = self.oi_func
        category = self.category
        interval = "4h"
        symbol   = self.symbol

        open_interest_path = self.get_open_interest_path()
        open_interest_csv  = f"{open_interest_path}/{symbol}.csv"

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
                self._send_tg_msg_to_backtest_channel(message)

            print(strategy, symbol, interval, category, function, exchange, "response = failed backtest, reason = without open_interest data file")
            print("**************************************************")

            raise StopIteration

        return open_interest_df

    def get_open_interest_path(self):
        interval = "4h"

        open_interest_path = f"D:/data/{self.asset}/{self.exchange}/{self.oi_func}/{self.category}/{interval}"

        return open_interest_path

    def _get_price_fr_df(self):
        funding_rate_df = self._get_funding_rate_df()
        price_df        = self._get_price_df()

        price_fr_df = pd.merge(funding_rate_df, price_df, on = "time", how = "inner")
        price_fr_df.rename(columns = {"datetime_x": "datetime"}, inplace = True)
        price_fr_df = price_fr_df[["time", "datetime", "open", "funding_rate"]]

        return price_fr_df

    def get_formatted_backtest_df(self):
        open_interest_df = self.get_open_interest_df()
        price_fr_df      = self._get_price_fr_df()

        backtest_df = pd.merge(open_interest_df, price_fr_df, on = "time", how = "inner")
        backtest_df.rename(columns = {"datetime_x": "datetime"}, inplace=True)

        backtest_df["fr_oi"] = backtest_df["funding_rate"] / backtest_df["open_interest"]

        backtest_df = backtest_df[["time", "datetime", "open", "funding_rate", "fr_oi"]]
        backtest_df = self._clean_data(backtest_df)

        return backtest_df

    def _clean_data(self, df):
        df.dropna(inplace = True)  # drop Nan value
        # df = df[df != 0].dropna()  # drop "0" value
        df = df.reset_index(drop = True)  # reset row index

        return df

    def _send_tg_msg_to_backtest_channel(self, message):
        base_url = "https://api.telegram.org/bot6233469935:AAHayu1tVZ4NleqRFM-61F6VQObWMCwF90U/sendMessage?chat_id=-809813823&text="
        requests.get(base_url + message)

    def _get_file_list(self, path):
        file_list = []

        for file in os.listdir(path):
            if os.path.isfile(os.path.join(path, file)):
                file = os.path.splitext(file)[0]
                file_list.append(file)

            else:
                file_list.append(file)

        return file_list