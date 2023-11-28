import matplotlib.pyplot as plt
import pandas as pd

class BybitCurveDrawer:
    def __init__(self, strategy, category, interval, symbol, single_df_name, rolling_window, upper_band, lower_band):
        self.asset      = "cryptocurrency"
        self.exchange   = "bybit"
        self.strategy   = strategy
        self.category   = category
        self.interval   = interval
        self.symbol     = symbol
        self.price_func = "kline"

        self.single_df_name = single_df_name
        self.rolling_window = rolling_window
        self.upper_band     = upper_band
        self.lower_band     = lower_band

    def _draw_curves(self):
        single_result_df = self._get_single_result_df()
        self._draw_and_save_equity_curve(single_result_df)

        print(f"{self.exchange}丨{self.strategy}丨{self.category}丨{self.interval}丨{self.symbol}丨"
              f"{self.single_df_name}丨({self.rolling_window}, {self.upper_band}, {self.lower_band})丨action = drew and saved equity curve")

        full_result_df = self._get_full_result_df()
        table_list     = self._get_sharpe_distribution_table_list(full_result_df)
        
        for table_dict in table_list:
            self._draw_and_save_sharpe_distribution_table(table_dict)

        print(f"{self.exchange}丨{self.strategy}丨{self.category}丨{self.interval}丨{self.symbol}丨"
              f"{self.single_df_name}丨({self.rolling_window}, {self.upper_band}, {self.lower_band})丨action = drew and saved sharpe distribution table")

    def _get_result_path(self):
        result_path = f"D:/backtest/{self.asset}/{self.exchange}/{self.strategy}/{self.category}/{self.interval}"

        return result_path

    def _get_single_result_df(self):
        result_path        = self._get_result_path()
        single_result_path = f"{result_path}/{self.symbol}/{self.single_df_name}/single_result"
        single_result_csv  = f"{single_result_path}/{self.symbol}_{self.rolling_window}_{self.upper_band}_{self.lower_band}.csv"

        single_result_df = pd.read_csv(single_result_csv)
        single_result_df["total_pnl"] = single_result_df["long_trading_pnl"] + single_result_df["short_trading_pnl"] + \
                                        single_result_df["long_fr_pnl"] + single_result_df["short_fr_pnl"] + \
                                        single_result_df["long_tx_fee"] + single_result_df["short_tx_fee"]

        single_result_df["cum_pnl"] = single_result_df["total_pnl"].cumsum()

        return single_result_df

    def _get_full_result_df(self):
        result_path      = self._get_result_path()
        full_result_path = f"{result_path}/{self.symbol}/{self.single_df_name}/full_result"
        full_result_csv  = f"{full_result_path}/{self.symbol}.csv"
        full_result_df   = pd.read_csv(full_result_csv)

        return full_result_df

    def _draw_and_save_equity_curve(self, single_result_df):
        equity_curve_data = single_result_df["cum_pnl"]

        plt.figure(figsize = (12, 6))
        plt.plot(single_result_df.index, equity_curve_data, label = "Cumulative PnL")
        plt.xlabel("Date")
        plt.ylabel("Equity")
        plt.title("Equity Curve")
        plt.legend()
        plt.grid(True)

        equity_curve_path = self._get_equity_curve_path()
        plt.savefig(equity_curve_path)

    def _get_equity_curve_path(self):
        result_path       = self._get_result_path()
        full_result_path  = f"{result_path}/{self.symbol}/{self.single_df_name}/full_result"
        equity_curve_path = f"{full_result_path}/{self.symbol}_{self.rolling_window}_{self.upper_band}_{self.lower_band}_equity_curve.png"

        return equity_curve_path
    
    def _get_sharpe_distribution_table_list(self, full_result_df):
        para_a = "rolling_window"
        para_b = "upper_band"
        para_c = "lower_band"

        table_dict = {}
        table_list = []

        table_ab = full_result_df[(full_result_df["lower_band"] == self.lower_band)]
        table_ab = table_ab.pivot(index = para_a, columns = para_b, values = "strat_sharpe")
        table_dict["table"]  = table_ab
        table_dict["para_a"] = para_a
        table_dict["para_b"] = para_b
        table_list.append(table_dict)

        table_dict = {}
        table_ac = full_result_df[(full_result_df["upper_band"] == self.upper_band)]
        table_ac = table_ac.pivot(index = para_a, columns = para_c, values = "strat_sharpe")
        table_dict["table"]  = table_ac
        table_dict["para_a"] = para_a
        table_dict["para_b"] = para_c
        table_list.append(table_dict)

        table_dict = {}
        table_bc = full_result_df[(full_result_df["rolling_window"] == self.rolling_window)]
        table_bc = table_bc.pivot(index = para_b, columns = para_c, values = "strat_sharpe")
        table_dict["table"]  = table_bc
        table_dict["para_a"] = para_b
        table_dict["para_b"] = para_c
        table_list.append(table_dict)
        
        return table_list
    
    def _draw_and_save_sharpe_distribution_table(self, table_dict):
        table  = table_dict["table"]
        para_x = table_dict["para_a"]
        para_y = table_dict["para_b"]

        plt.figure(figsize = (20, 8))
        plt.imshow(table, cmap = "PuBu", aspect = "auto")
        plt.colorbar()
        plt.title("Sharpe Ratio Distribution")
        plt.xlabel(para_y)
        plt.ylabel(para_x)
        plt.xticks(range(len(table.columns)), table.columns, rotation = "vertical")
        plt.yticks(range(len(table.index)), table.index)

        for i in range(len(table.index)):
            for j in range(len(table.columns)):
                plt.text(j, i, f"{table.iloc[i, j]:.2f}", ha = "center", va = "center", color = "black")

        sharpe_table_path = self._get_sharpe_table_path(para_x, para_y)
        plt.savefig(sharpe_table_path)

        """
        plt.show()
        
        # PuBu -> Blue, Yellow, for investors
        # BuGn -> Green, for investors

        # RdYlGn -> Green, Yellow, Red -> for myself
        # YlGn   -> Green, Yellow      -> for myself
        """

    def _get_sharpe_table_path(self, para_a, para_b):
        result_path       = self._get_result_path()
        full_result_path  = f"{result_path}/{self.symbol}/{self.single_df_name}/full_result"
        sharpe_table_path = f"{full_result_path}/{self.symbol}_{para_a}_{para_b}.png"

        return sharpe_table_path

    """
    def create_sharpe_ratio_surface(self, result_df, x_para, y_para, z_para, title):
        x_values     = result_df[x_para]
        y_values     = result_df[y_para]
        z_values     = result_df[z_para]
        sharpe_ratio = result_df["strat_sharpe"]

        fig = plt.figure()
        ax  = fig.add_subplot(111, projection = "3d")

        sc = ax.scatter(x_values, y_values, z_values, c = sharpe_ratio, cmap = "viridis")

        ax.set_xlabel(x_para)
        ax.set_ylabel(y_para)
        ax.set_zlabel(z_para)
        ax.set_title(title)
        plt.colorbar(sc, label = "Sharpe Ratio")

        plt.show()
        """