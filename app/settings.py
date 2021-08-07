import logging
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME")

# env variables
# binance test api account: https://testnet.binance.vision/
BINANCE_KEY = os.getenv("TEST_BINANCE_API_KEY")
BINANCE_SECRET = os.getenv("TEST_BINANCE_API_SECRET_KEY")

log_file_path = os.path.abspath(os.path.join(".", os.pardir)) + "/logs/log"
LOG_FORMAT = "%(levelname)s %(filename)s line:%(lineno)d %(asctime)s - %(message)s"
logging.basicConfig(filename=log_file_path, level=logging.INFO, format=LOG_FORMAT)

TIMESTREAM_DATABASE = "coinmove"
TIMESTREAM_TABLE = "technical_data"
TIMESTREAM_TEST_TABLE = "price"

SQLITE_DATABASE = "../coinmove.db"
SQLITE_TABLE = "technical_data"

INTERVALS = ["5m", "1h", "4h", "12h", "1d"]

OHLCV_QUEUE = "ticker-ohlcv.fifo"