from functools import partial
import os
import requests
from time import time

from binance.client import Client
from binance.websockets import BinanceSocketManager

import settings
from app.exchange import Binance
from app.timestream import Timestream


# binance test api account: https://testnet.binance.vision/
api_key = settings.BINANCE_KEY
api_secret = settings.BINANCE_SECRET

try:
    client = Client(api_key, api_secret)
except requests.exceptions.ConnectionError as conn_err:
    print("Baglanamıyor: ", conn_err)

client.API_URL = "https://testnet.binance.vision/api"


binance = Binance()
bsm = BinanceSocketManager(client)
ts = Timestream(database=settings.TIMESTREAM_DATABASE, table=settings.TIMESTREAM_TABLE)

def kline_callback(response, coin:str, currency:str, exchange:str):
    if response["e"] == "error":
        return response["e"]
    if not response["k"]["x"]:
        return None

    records = ts.build_records(response, coin, currency, exchange)
    if records:
        ts.insert(records)

    #print(result)


binance_result = binance.get_all_tickers(margin=True)
binance_tickers = binance_result["tickers"]
exchange = binance_result["exchange"]
bsm_result = {}

for interval in settings.INTERVALS:
    for ticker in binance_tickers:
        ticker_arr = ticker.split("_")
        kline_wrapper = partial(kline_callback, coin=ticker_arr[0], currency=ticker_arr[1], exchange=exchange)
        bsm_result[ticker + "-" + interval] = bsm.start_kline_socket(ticker.replace("_",""), kline_wrapper, interval=interval)

bsm.start()

