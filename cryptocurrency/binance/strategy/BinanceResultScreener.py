"""
1) >2.5 -> acceptable result for 2nd round screening
2) 0    -> unacceptable result for file deletion
3) -1   -> bug in backtesting that need to re-run the symbol into the backtesting engine
4) -2   -> bug in BacktestSystem that need to be solved
"""

import datetime
import numpy as np
import os
import pandas as pd
from pprint import pprint
import requests
import time

class BinanceResultScreener:
    def __init__(self, strategy, instrument, product, interval):
        self.asset      = "cryptocurrency"
        self.exchange   = "binance"
        self.strategy   = strategy
        self.instrument = instrument
        self.product    = product
        self.interval   = interval

    def _generate_full_symbol_backtest_report(self):
        original_symbol_list_path = self._get_original_symbol_list_path()
        finished_symbol_list_path = self.get_finished_symbol_list_path()

        original_symbol_list = self._get_original_symbol_list()
        finished_symbol_list = self._get_finished_symbol_list()

        result_list = self._generate_result_list(original_symbol_list, finished_symbol_list, finished_symbol_list_path)
        result_df   = self._generate_result_df(result_list)
        self._store_result_df(result_df)

        print(f"{self.exchange}丨{self.strategy}丨{self.instrument}丨{self.product}丨{self.interval}丨action = exported full_symbol_backtest_result to csv")

    def _get_file_list(self, path):
        file_list = []
    
        try:
            for file in os.listdir(path):
                if os.path.isfile(os.path.join(path, file)): # for csv files
                    file = os.path.splitext(file)[0]
                    file_list.append(file)

                else: # for folders
                    file_list.append(file)
        except:
            pass
    
        return file_list
    
    def _get_original_symbol_list_path(self):
        price_function = "kline"

        original_symbol_list_path = f"D:/data/{self.asset}/{self.exchange}/{self.instrument}/{self.product}/{price_function}/{self.interval}"

        return original_symbol_list_path
    
    def _get_original_symbol_list(self):
        original_symbol_list_path = self._get_original_symbol_list_path()
        original_symbol_list      = self._get_file_list(original_symbol_list_path)

        backtested_symbol_list = []

        for original_symbol in original_symbol_list:
            if (original_symbol[-6:].isdigit() == False) and (original_symbol != "None"): # For current moment not trading delivery contracts
                backtested_symbol_list.append(original_symbol)

        return backtested_symbol_list
    
    def get_finished_symbol_list_path(self):
        finished_symbol_list_path = f"D:/backtest/{self.asset}/{self.exchange}/{self.strategy}/{self.instrument}/{self.product}/{self.interval}"

        return finished_symbol_list_path

    def _get_finished_symbol_list(self):
        finished_symbol_list_path = self.get_finished_symbol_list_path()
        finished_symbol_list      = self._get_file_list(finished_symbol_list_path)

        return finished_symbol_list

    def _generate_result_list(self, original_symbol_list, finished_symbol_list, finished_symbol_list_path):
        asset      = self.asset
        strategy   = self.strategy
        exchange   = self.exchange
        instrument = self.instrument
        product    = self.product
        interval   = self.interval

        num_of_trade_list = []
        win_rate_list     = []
        ann_return_list   = []
        mdd_list          = []
        calmar_list       = []

        highest_sharpe_list = []
        # lowest_sharpe_list  = []

        # contrary_test_list = []
        rubbish_strat_list = []

        result_list = []

        for symbol in original_symbol_list:
            if symbol in finished_symbol_list:
                symbol_pass = False

                symbol_csv = f"{finished_symbol_list_path}/{symbol}/full_result/{symbol}.csv"
                symbol_df  = pd.read_csv(symbol_csv)

                for i in range(len(symbol_df)):
                    highest_sharpe = symbol_df["strat_sharpe"].iloc[i]
                    num_of_trade   = symbol_df["num_of_trade"].iloc[i]

                    if (highest_sharpe > 2.5) and (num_of_trade > 150):
                        symbol_pass = True

                        # lowest_sharpe  = symbol_df["strat_sharpe"].iloc[-1]
                        win_rate     = symbol_df["strat_win_rate"].iloc[i]
                        ann_return   = symbol_df["strat_ann_return"].iloc[i]
                        mdd          = symbol_df["strat_mdd"].iloc[i]
                        calmar       = symbol_df["strat_calmar"].iloc[i]

                        num_of_trade_list.append(num_of_trade)
                        win_rate_list.append(win_rate)
                        ann_return_list.append(ann_return)
                        mdd_list.append(mdd)
                        calmar_list.append(calmar)
                        highest_sharpe_list.append(highest_sharpe)
                        # lowest_sharpe_list.append(lowest_sharpe)

                        """
                        if (lowest_sharpe * -1) > highest_sharpe:
                            contrary_test = True
                            contrary_test_list.append(contrary_test)
                            
                        else:
                            contrary_test = False
                            contrary_test_list.append(contrary_test)
                            """

                        if highest_sharpe < 1.5:
                            rubbish_strat = True
                            rubbish_strat_list.append(rubbish_strat)

                        else:
                            rubbish_strat = False
                            rubbish_strat_list.append(rubbish_strat)

                        print(f"{self.exchange}丨{self.strategy}丨{self.instrument}丨{self.product}丨{self.interval}丨{self.symbol}丨response = fulfilled the requirement")
                        break

                    else:
                        print(f"{self.exchange}丨{self.strategy}丨{self.instrument}丨{self.product}丨{self.interval}丨{self.symbol}丨response = failed to fulfill the requirement")
                        pass

                if symbol_pass == False:
                    num_of_trade   = -1
                    win_rate       = -1
                    ann_return     = -1
                    mdd            = -1
                    calmar         = -1
                    highest_sharpe = -1
                    # lowest_sharpe  = -1
                    # contrary_test  = True
                    rubbish_strat  = True

                    num_of_trade_list.append(num_of_trade)
                    win_rate_list.append(win_rate)
                    ann_return_list.append(ann_return)
                    mdd_list.append(mdd)
                    calmar_list.append(calmar)
                    highest_sharpe_list.append(highest_sharpe)
                    # lowest_sharpe_list.append(lowest_sharpe)
                    # contrary_test_list.append(contrary_test)
                    rubbish_strat_list.append(rubbish_strat)

                print(f"{self.exchange}丨{self.strategy}丨{self.instrument}丨{self.product}丨{self.interval}丨{self.symbol}丨response = generated single backtest report")
                print("**************************************************")

            else:
                num_of_trade   = 0
                win_rate       = 0
                ann_return     = 0
                mdd            = 0
                calmar         = 0
                highest_sharpe = 0
                # lowest_sharpe  = 0
                # contrary_test  = True
                rubbish_strat  = False

                num_of_trade_list.append(num_of_trade)
                win_rate_list.append(win_rate)
                ann_return_list.append(ann_return)
                mdd_list.append(mdd)
                calmar_list.append(calmar)
                highest_sharpe_list.append(highest_sharpe)
                # lowest_sharpe_list.append(lowest_sharpe)
                # contrary_test_list.append(contrary_test)
                rubbish_strat_list.append(rubbish_strat)

                print(f"{self.exchange}丨{self.strategy}丨{self.instrument}丨{self.product}丨{self.interval}丨{self.symbol}丨response = without single backtest report")
                print("**************************************************")

        result_list.append(original_symbol_list)
        result_list.append(num_of_trade_list)
        result_list.append(win_rate_list)
        result_list.append(ann_return_list)
        result_list.append(mdd_list)
        result_list.append(calmar_list)
        result_list.append(highest_sharpe_list)
        # result_list.append(lowest_sharpe_list)
        # result_list.append(contrary_test_list)
        result_list.append(rubbish_strat_list)

        return result_list

    def _generate_result_df(self, result_list):
        original_symbol_list = result_list[0]
        num_of_trade_list    = result_list[1]
        win_rate_list        = result_list[2]
        ann_return_list      = result_list[3]
        mdd_list             = result_list[4]
        calmar_list          = result_list[5]
        highest_sharpe_list  = result_list[6]
        # lowest_sharpe_list   = result_list[7]
        # contrary_test_list   = result_list[7]
        rubbish_strat_list   = result_list[7]

        original_symbol_df = pd.DataFrame(original_symbol_list)
        num_of_trade_df    = pd.DataFrame(num_of_trade_list)
        win_rate_df        = pd.DataFrame(win_rate_list)
        ann_return_df      = pd.DataFrame(ann_return_list)
        mdd_df             = pd.DataFrame(mdd_list)
        calmar_df          = pd.DataFrame(calmar_list)
        highest_sharpe_df  = pd.DataFrame(highest_sharpe_list)
        # lowest_sharpe_df   = pd.DataFrame(lowest_sharpe_list)
        # contrary_test_df   = pd.DataFrame(contrary_test_list)
        rubbish_strat_df   = pd.DataFrame(rubbish_strat_list)

        result_df = pd.concat((original_symbol_df, num_of_trade_df, win_rate_df, ann_return_df, mdd_df, \
                               calmar_df, highest_sharpe_df, rubbish_strat_df), axis = 1)

        column_name_list  = ["symbol", "num_of_trade", "win_rate", "ann_return", "mdd", \
                             "calmar","highest_sharpe", "rubbish_strat"]
        result_df.columns = column_name_list

        result_df = result_df.sort_values(by = "highest_sharpe", ascending = False)

        return result_df

    def _store_result_df(self, result_df):
        result_csv = f"D:/backtest/{self.asset}/{self.strategy}/{self.exchange}/{self.instrument}/{self.product}/{self.interval}/full_symbol_backtest_result.csv"
        result_df.to_csv(result_csv, index=False)

"""
def main():
    binanceResultScreener = BinanceResultScreener("claire", "futures", "usdm_futures", "8h")
    binanceResultScreener._get_original_symbol_list()

if __name__ == "__main__":
    main()
    """