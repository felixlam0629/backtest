import pandas as pd
import matplotlib.pyplot as plt

class BinanceCurveDrawer:
    def __init__(self):
        self.asset      = "cryptocurrency"
        self.exchange   = "binance"
        self.strategy   = "testing_basis_index_price_open_interest"
        self.instrument = "futures"
        self.product    = "coinm_futures"
        self.interval   = "8h"
        self.symbol     = "BTCUSD_PERP"

        self.rolling_window = 10
        self.upper_band     = 1
        self.lower_band     = 1

    def draw_curves(self):
        result_df = self._get_result_df()
        self._draw_equity_curve(result_df)

    def get_result_path(self):
        result_path = f"D:/backtest/{self.asset}/{self.exchange}/{self.strategy}/{self.instrument}/{self.product}/{self.interval}"

        return result_path

    def _get_result_df(self):
        result_path = self.get_result_path()
        result_csv  = f"{result_path}/{self.symbol}/single_result/{self.symbol}_{self.rolling_window}_{self.upper_band}_{self.lower_band}.csv"

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
        symbol         = self.symbol
        rolling_window = self.rolling_window
        upper_band     = self.upper_band
        lower_band     = self.lower_band

        result_path = self.get_result_path()

        equity_curve_path  = f"{result_path}/{self.symbol}/full_result/{self.symbol}_{self.rolling_window}_{self.upper_band}_{self.lower_band}.png"

        return equity_curve_path

def main():
    binanceCurveDrawer = BinanceCurveDrawer()
    binanceCurveDrawer.draw_curves()

if __name__ == "__main__":
    main()