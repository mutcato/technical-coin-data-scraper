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
TIMESTREAM_DATABASE_TEST = "coinmove_test"
TIMESTREAM_TABLE = "technical_data"
TIMESTREAM_TABLE_TEST = "technical_data_test"

DYNAMO_TABLE = "metrics"
DYNAMO_TABLE_TEST = "metrics_test"
DYNAMO_SUMMARY_TABLE = "metrics_summary"

INTERVALS = ["5m", "1h", "4h"]

OHLCV_QUEUE = "ticker-ohlcv"