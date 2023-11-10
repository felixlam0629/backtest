import matplotlib.pyplot as plt
import pandas as pd

class BybitCurveDrawer:
    def __init__(self, strategy, category, interval, symbol, rolling_window, upper_band, lower_band):
        self.asset      = "Cryptocurrency"
        self.exchange   = "bybit"
        self.strategy   = strategy
        self.category   = category
        self.interval   = interval
        self.symbol     = symbol
        self.price_func = "kline"

        self.rolling_window = rolling_window
        self.upper_band     = upper_band
        self.lower_band     = lower_band

    def _draw_curves(self):
        result_df = self._get_result_df()
        self._draw_equity_curve(result_df)

    def _get_result_path(self):
        result_path = f"D:/backtest/{self.asset}/{self.exchange}/{self.strategy}/{self.category}/{self.interval}"

        return result_path

    def _get_result_df(self):
        result_path = self._get_result_path()
        result_csv  = f"{result_path}/{self.symbol}/backtest_set/single_result/{self.symbol}_{self.rolling_window}_{self.upper_band}_{self.lower_band}.csv"

        result_df = pd.read_csv(result_csv)
        result_df["total_pnl"] = result_df["long_trading_pnl"] + result_df["short_trading_pnl"] + \
                                 result_df["long_fr_pnl"] + result_df["short_fr_pnl"] + \
                                 result_df["long_tx_fee"] + result_df["short_tx_fee"]

        result_df["cum_pnl"] = result_df["total_pnl"].cumsum()

        return result_df

    def _draw_equity_curve(self, result_df):
        equity_curve_data = result_df["cum_pnl"]

        plt.figure(figsize = (12, 6))
        plt.plot(result_df.index, equity_curve_data, label = "Cumulative PnL")
        plt.xlabel("Date")
        plt.ylabel("Equity")
        plt.title("Equity Curve")
        plt.legend()
        plt.grid(True)

        equity_curve_path = self.get_equity_curve_path()
        plt.savefig(equity_curve_path)

        plt.show()

    def _get_equity_curve_path(self):
        result_path       = self._get_result_path()
        equity_curve_path = f"{result_path}/{self.symbol}/backtest_set/full_result/{self.symbol}_{self.rolling_window}_{self.upper_band}_{self.lower_band}.png"

        return equity_curve_path