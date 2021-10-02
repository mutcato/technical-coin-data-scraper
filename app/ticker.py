import sqlite3
from time import time
from typing import Dict
import settings
from db import sqlite, timestream, queue

from helpers import convert_interval_to_seconds_int

logger = settings.logging.getLogger()

class Ticker:
    def __init__(self, kline_response:Dict, coin, currency, exchange):
        """
        kline_response = {
                "e": "kline",                           # event type
                "E": 1499404907056,                     # event time
                "s": "ETHBTC",                          # symbol
                "k": {
                    "t": 1499404860000,                 # start time of this bar
                    "T": 1499404919999,                 # end time of this bar
                    "s": "ETHBTC",                      # symbol
                    "i": "1m",                          # interval
                    "f": 77462,                         # first trade id
                    "L": 77465,                         # last trade id
                    "o": "0.10278577",                  # open
                    "c": "0.10278645",                  # close
                    "h": "0.10278712",                  # high
                    "l": "0.10278518",                  # low
                    "v": "17.47929838",                 # volume
                    "n": 4,                             # number of trades
                    "x": false,                         # whether this bar is final
                    "q": "1.79662878",                  # quote volume
                    "V": "2.34879839",                  # volume of active buy
                    "Q": "0.24142166",                  # quote volume of active buy
                    "B": "13279784.01349473"    # can be ignored
                    }
            }
        """
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
        self.measures["volume"] = self.response["k"]["v"]
        self.measures["number_of_trades"] = self.response["k"]["n"]

    def insert(self):
        self.build_record()
        sqs = queue.SQS()
        sqs.insert(ticker=self)
        stream = timestream.Stream(database=settings.TIMESTREAM_DATABASE, table=settings.TIMESTREAM_TABLE)
        stream.insert(ticker=self)
        # sqlite_table = sqlite.Table()
        # sqlite_table.insert(ticker=self)
