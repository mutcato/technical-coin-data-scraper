import sqlite3
from time import time
from typing import Dict
import settings
from db import sqlite

from helpers import convert_interval_to_seconds_int
from indicators import IndicatorException, Sequence

logger = settings.logging.getLogger()

class Ticker:
    def __init__(self, kline_response:Dict, coin, currency, exchange):
        self.coin = coin
        self.currency = currency
        self.interval = kline_response["k"]["i"]
        self.time = int(str(kline_response["k"]["T"])[:-3])
        self.exchange = exchange
        self.response = kline_response
        self.measures = {}

    def build_record(self):
        self.measures["open"] = self.response["k"]["o"]
        self.measures["high"] = self.response["k"]["h"]
        self.measures["low"] = self.response["k"]["l"]
        self.measures["close"] = self.response["k"]["c"]
        self.measures["volume"] = self.response["k"]["c"]
        try:
            last_bars = self.get_last_x_bar()
            sequence = Sequence(last_bars)
            self.measures["rsi"] = "{:.2f}".format(sequence.rsi()[-1])
            self.measures["mfi"] = "{:.2f}".format(sequence.mfi()[-1])
            self.measures["cci"] = "{:.2f}".format(sequence.cci()[-1])
            self.measures["kama"] = "{:.2f}".format(sequence.kama()[-1])
            self.measures["inv_rsi"] = "{:.2f}".format(sequence.inverse_fisher_transform(indicator="rsi", normalized=True)[-1])
            self.measures["inv_cci"] = "{:.2f}".format(sequence.inverse_fisher_transform(indicator="cci", normalized=True)[-1])
            self.measures["inv_mfi"] = "{:.2f}".format(sequence.inverse_fisher_transform(indicator="mfi", normalized=True)[-1])
            macd = sequence.macd()
            self.measures["macd"] = "{:.2f}".format(macd[0][-1])
            self.measures["macd_signal"] = "{:.2f}".format(macd[1][-1])
            self.measures["macd_diff"] = "{:.2f}".format(macd[2][-1])
            ichimoku = sequence.ichimoku()
            self.measures["senkou_span_a"] = "{:.2f}".format(ichimoku[0][-1])
            self.measures["senkou_span_b"] = "{:.2f}".format(ichimoku[1][-1])
            self.measures["kijun_sen"] = "{:.2f}".format(ichimoku[2][-1])
            self.measures["tenkan_sen"] = "{:.2f}".format(ichimoku[3][-1])
            self.measures["atr"] = "{:.2f}".format(sequence.atr()[-1])
        except IndicatorException as e:
            logger.warning(f"Warning: Indicator for {self.coin}_{self.currency}: {e}")              
        except Exception as err:
            logger.error(f"There is no record in database for ticker: {self.coin}_{self.currency}, interval: {self.interval} Increase time interval. KeyError:{err}")
 

    def get_last_x_bar(self, x:int=21):
        connection = sqlite3.connect(settings.SQLITE_DATABASE)
        cursor = connection.cursor()
        query = f"""SELECT * FROM {settings.SQLITE_TABLE} 
        WHERE ticker="{self.coin}_{self.currency}" 
        AND interval="{self.interval}" 
        AND time > {int(time()) - convert_interval_to_seconds_int(self.interval)*x}
        ORDER BY time ASC"""

        measures = cursor.execute(query).fetchall()
        cursor.close()

        result = {}
        result["close"], result["high"], result["low"], result["volume"], result["time"]  = [], [], [], [], []
        for measure in measures:
            if "close" in measure:
                result["close"].append(measure[4])
                result["time"].append(measure[5])
            if "high" in measure:
                result["high"].append(measure[4])
            if "low" in measure:
                result["low"].append(measure[4])
            if "volume" in measure:
                result["volume"].append(measure[4])

        print(result)
        return result

    def insert(self):
        self.build_record()
        #Timestream.insert(self)
        sqlite_table = sqlite.Table()
        sqlite_table.insert(ticker=self)
