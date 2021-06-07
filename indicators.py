
import logging
import numpy as np
import pandas as pd

import ta

logger = logging.getLogger()

class Sequence:
    def __init__(self, sequence):
        """
        sequence->dict of lists for a currency: {"close":[12,13,14], "high": [14,15,16], ...}
        self.candle_data: 
                open      high     rsi      volume     close       low  number_of_trades     cci
        0    59681.21  59740.96   49.09  304.724841  59737.85  59500.00           10885.0    0.00
        1    59733.74  59830.00  100.00  155.527422  59760.00  59700.00            6620.0    0.00
        2    59770.36  59800.00   88.92  227.266576  59744.89  59700.00            8444.0    0.00
        3    59744.98  59877.87   88.54  290.998111  59855.74  59625.34            8973.0    0.00
        4    59855.74  59959.09   88.91  255.823861  59917.02  59813.23           10039.0    0.00
        ..        ...       ...     ...         ...       ...       ...               ...     ...
        101  61045.66  61185.00   47.53  128.048140  61150.01  61010.34            6650.0    0.00
        102  61150.01  61180.00   54.78  105.201355  61096.04  61039.73            6587.0    0.00
        103  61091.75  61299.00   53.62  172.732334  61220.50  61089.98            5927.0    0.00
        104  61376.38  61387.11   60.82  242.787754  61237.02  61150.00            6149.0    0.00
        105  61238.44  61400.00   65.96  439.718629  61295.27  61198.84            6331.0  177.03
        """
        self.sequence = sequence
        self.fill_empty_cells() 
        try:
            self.candle_data = pd.DataFrame(self.sequence)
        except ValueError as err:
            logger.error(f"Message: {err}. Look at the list lengths --> {self.sequence}")

    def fill_empty_cells(self):
        """
        if all columns are not in the same sizes makes them same by filling earlier cells with zeros 
        makes dictionary's related list something like this:
        self.sequence["cci"] = [0.00, 0.00, 0.00, 0.00, 0.00, ..., 0.00, 0.00, 0.00, 0.00, 177.03]
        """
        for key in self.sequence:
            if len(self.sequence[key]) < self.longest_array_size:
                self.sequence[key] = [0] * (self.longest_array_size - len(self.sequence[key])) + self.sequence[key]

    @property
    def longest_array_size(self):
        return max([len(self.sequence[key]) for key in self.sequence])

    def rsi(self, period=14):
        """
        period: how many bar will you get, int
        """
        result = ta.momentum.RSIIndicator(self.candle_data["close"], window=period, fillna=True)
        return result.rsi().tolist()

    def cci(self, period=20):
        """
        self.closes: close values of n bar, list [oldest, ....., newest]
        period: how many bar will you get, int
        """
        result = ta.trend.CCIIndicator(self.candle_data["high"], self.candle_data["low"], self.candle_data["close"], window=period, fillna=True)
        return result.cci().tolist()

    def mfi(self, period=14):
        result = ta.volume.MFIIndicator(self.candle_data["high"], self.candle_data["low"], self.candle_data["close"], self.candle_data["volume"], window=period, fillna=True)
        return result.money_flow_index().tolist()

    def atr(self, period=14):
        result = ta.volatility.AverageTrueRange(self.candle_data["high"], self.candle_data["low"], self.candle_data["close"], window=period, fillna=True)
        return result.average_true_range().tolist()

    def kama(self, period=14):
        """
        Kaufman's Adaptive moving Average
        """
        result = ta.momentum.KAMAIndicator(self.candle_data["close"], window=period, fillna=True)
        return result.kama().tolist()

    def macd(self):
        result = ta.trend.MACD(self.candle_data["close"], fillna=True)
        macd = result.macd().tolist()
        macd_signal = result.macd_signal().tolist()
        macd_diff = result.macd_diff().tolist()
        return macd, macd_signal, macd_diff

    def ichimoku(self):
        # https://technical-analysis-library-in-python.readthedocs.io/en/latest/ta.html?highlight=ichimoku#ta.trend.IchimokuIndicator
        result = ta.trend.IchimokuIndicator(high=self.candle_data["high"], low=self.candle_data["low"], fillna=True)
        ichimoku_senkou_span_a = result.ichimoku_a().tolist()
        ichimoku_senkou_span_b = result.ichimoku_b().tolist()
        ichimoku_kijun_sen = result.ichimoku_base_line().tolist()
        ichimoku_tenkan_sen = result.ichimoku_conversion_line().tolist()
        return (
            ichimoku_senkou_span_a,
            ichimoku_senkou_span_b,
            ichimoku_kijun_sen,
            ichimoku_tenkan_sen
        )

    def inverse_fisher_transform(self, indicator, period=14, normalized=False):
        """
        Inverse transforms oscillates between -1 and 1
        Normalized versions oscillates 0 and 100
        """
        # https://github.com/freqtrade/freqtrade-strategies/blob/master/user_data/strategies/Strategy005.py
        # https://www.prorealcode.com/prorealtime-indicators/inverse-fisher-transform-rsi/
        calculate_indicator = getattr(self, indicator)
        ind = np.array(calculate_indicator(period))
        x=0.1*(ind-50)
        
        #Inverse transform of RSI. Between -1 and 1
        y=(np.exp(2*x)-1)/(np.exp(2*x)+1)

        if normalized == False:
            return y.tolist()

        #Normalized version is between 0 and 100
        y_norm =  50*(y+1)

        return y_norm.tolist()