import requests
from typing import Dict, List

from binance.client import Client
from binance.exceptions import BinanceAPIException
from dotenv import load_dotenv

import settings


def snake_case(string1, string2):
    return string1.replace(string2, "_") + string2


class Binance:
    def __init__(self):
        self.client = Client(settings.BINANCE_KEY, settings.BINANCE_SECRET)

    def get_close_prices(
        self, symbol, interval: str, start: int, end: int, currency: str = "USDT"
    ) -> Dict:
        candles = self.client.get_klines(
            symbol=symbol + currency, interval=interval, startTime=start, endTime=end
        )
        result = {candle[6] + 1: candle[4] for candle in candles}
        return result

    def get_all_tickers(self, margin: bool = True, currency: bool = False):
        """ If margin is true gets only margin trade enabled coins """

        result = requests.get("https://api.binance.com/api/v3/exchangeInfo")
        coins = result.json()["symbols"]
        filtered_coins = coins
        if currency:
            filtered_coins = [coin for coin in coins if coin["quoteAsset"] == currency]

        if not margin:
            tickers = [
                snake_case(coin["symbol"], coin["quoteAsset"])
                for coin in filtered_coins
            ]
            return {"exchange": "binance", "tickers": tickers}
        else:
            tickers = [
                snake_case(coin["symbol"], coin["quoteAsset"])
                for coin in filtered_coins
                if "MARGIN" in coin["permissions"]
            ]
            return {"exchange": "binance", "tickers": tickers}
