#!/usr/bin/python3
# coding: utf-8

from functools import partial
import requests

from binance.client import Client
from binance.websockets import BinanceSocketManager

import settings
from exchange import Binance
from ticker import Batch, Ticker

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


def kline_callback(response, coin: str, currency: str, exchange: str):
    if response["e"] == "error":
        return response["e"]
    if not response["k"]["x"]:
        return None

    ticker = Ticker(response, coin, currency, exchange)
    interval = response["k"]["i"]
    batch[interval].add(ticker)

    if batch[interval].length >= batch[interval].limit:
        batch[interval].insert_dynamo()
        batch[interval].insert_sqlite()
        batch[interval].empty()

    batch[interval].index = batch[interval].index + 1 

    if batch[interval].index >= batch[interval].last_ticker_index:
        batch[interval].reset_index()
        batch[interval].empty()


binance_result = binance.get_all_tickers(margin=False, currency="USDT")
binance_tickers = binance_result["tickers"]
binance_tickers_count = len(binance_tickers)

batch = {}
for interval in settings.INTERVALS:
    batch[interval] = Batch(number_of_tickers = binance_tickers_count, batch_size_limit=20)

exchange = binance_result["exchange"]
bsm_result = {}

print(f"binance_tickers_count: {binance_tickers_count}")
print(binance_tickers)
for interval in settings.INTERVALS:
    for ticker in binance_tickers:
        ticker_arr = ticker.split("_")
        kline_wrapper = partial(
            kline_callback,
            coin=ticker_arr[0],
            currency=ticker_arr[1],
            exchange=exchange,
        )
        bsm_result[ticker + "-" + interval] = bsm.start_kline_socket(
            ticker.replace("_", ""), kline_wrapper, interval=interval
        )

bsm.start()

# print("Starting connection close")
# for connection in bsm_result.items():
#     print(connection)
#     bsm.stop_socket(connection[1])
# print("End connection close")
