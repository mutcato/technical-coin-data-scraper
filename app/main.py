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
    batch.add(ticker)
    if batch.length > 20:
        batch.insert_dynamo()
        batch.empty()
        print("END OF 100")


batch = Batch()
binance_result = binance.get_all_tickers(margin=False, currency="USDT")
binance_tickers = binance_result["tickers"]
exchange = binance_result["exchange"]
bsm_result = {}

print(f"length: {len(binance_tickers)}")
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
