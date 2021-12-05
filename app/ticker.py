from datetime import datetime
from decimal import Decimal
from math import ceil
from time import time
from typing import Dict, List
import settings
from db import dynamo, sqlite, timestream


logger = settings.logging.getLogger()


class Ticker:
    TTL = {
        "5m": 120 * 24 * 60 * 60,
        "15m": 120 * 24 * 60 * 60 * 3,
        "1h": 120 * 24 * 60 * 60 * 12,
        "4h": 120 * 24 * 60 * 60 * 12 * 4,
        "8h": 120 * 24 * 60 * 60 * 12 * 8,
        "1d": 120 * 24 * 60 * 60 * 12 * 24,
    }

    def __init__(self, kline_response: Dict, coin, currency, exchange):
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
        self.measures["open"] = self.response["k"]["o"]
        self.measures["high"] = self.response["k"]["h"]
        self.measures["low"] = self.response["k"]["l"]
        self.measures["close"] = self.response["k"]["c"]
        self.measures["volume"] = self.response["k"]["v"]
        self.measures["number_of_trades"] = self.response["k"]["n"]

    def __str__(self):
        return self.coin + "_" + self.currency

    def convert_to_dynamo_item(self) -> dict:
        item = {
            "ticker_interval": f"{self.coin}_{self.currency}_{self.interval}",
            "time": datetime.utcfromtimestamp(self.time).strftime("%Y-%m-%d %H:%M:%S"),
            "open": Decimal(str(self.measures["open"])),
            "high": Decimal(str(self.measures["high"])),
            "low": Decimal(str(self.measures["low"])),
            "close": Decimal(str(self.measures["close"])),
            "volume": Decimal(str(self.measures["volume"])),
            "number_of_trades": int(self.measures["number_of_trades"]),
            "TTL": self.time + self.TTL[self.interval],
        }
        return item


class Batch:
    def __init__(self, number_of_tickers, batch_size_limit:int = 20):
        self.objects: List(Ticker) = []
        self.limit = batch_size_limit
        self.index = 1 # To fullfill batch.index == last_ticker_index condition has to be 1 not 0  !!!
        self.last_ticker_index = number_of_tickers

    def __list__(self):
        return self.objects

    @property
    def length(self):
        return len(self.objects)

    def add(self, ticker: Ticker):
        self.objects.append(ticker)

    def empty(self):
        del self.objects[:]

    def reset_index(self):
        self.index = 1

    def insert_dynamo(self):
        metrics = dynamo.Metrics()
        metrics.batch_insert(self.objects)

    def insert_sqlite(self):
        table = sqlite.Table()
        table.batch_insert(self.objects)

    def insert_timestream(self):
        table = timestream.Stream()
        table.batch_insert(self.objects)
