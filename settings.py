import logging
import os
from dotenv import load_dotenv

load_dotenv()

# env variables
# binance test api account: https://testnet.binance.vision/
BINANCE_KEY = os.getenv("TEST_BINANCE_API_KEY")
BINANCE_SECRET = os.getenv("TEST_BINANCE_API_SECRET_KEY")

LOG_FORMAT = "%(levelname)s %(filename)s line:%(lineno)d %(asctime)s - %(message)s"
logging.basicConfig(filename="logs/log", level=logging.INFO, format=LOG_FORMAT)

TIMESTREAM_DATABASE = "coindataforeverybody"
TIMESTREAM_TABLE = "technical-data"
TIMESTREAM_TEST_TABLE = "price"

INTERVALS = ["5m", "1h", "4h", "12h", "1d", "1w"]

