from decimal import Decimal
from typing import Dict
from settings import logging
import boto3

from indicators import Sequence

logger = logging.getLogger()

class Timestream:
    write_client = boto3.client("timestream-write")
    query_client = boto3.client("timestream-query")

    insertion_limit = 100 # you can insert this many records per time

    def __init__(self, database, table):
        self.database = database
        self.table = table

    def get_last_candles(self, time_period, interval):
        """
        time_period: time period you want candle records are in. str -> 6h
        interval: candle interval. str -> 5m
        measure: which MeasureValue you want. str -> close
        return dict -> last_prices = {"BTC": {"close": []}, {"time": [16213221,16876876]}, "ETH": {"close": []}, {"time": []}}
        """
        
        cls = self.__class__

        try:
            response = cls.query_client.query(
                QueryString = f"""SELECT currency, measure_name, measure_value::double, time FROM "{self.database}"."{self.table}" WHERE interval='{interval}' AND time >= ago({time_period}) ORDER BY time ASC"""
            )
        except self.query_client.exceptions.ValidationException as e:
            print(f"Warning: ValidationException: {e}")
            return None
        return self.serialize_into_array(response)
    
    @staticmethod
    def serialize_into_array(response):
        """
        params array of dicts: [{'Data': [{'ScalarValue': 'BTC'}, {'ScalarValue': 'low'}, {'ScalarValue': '56312.91'}, {'ScalarValue': '2021-03-13 00:30:00.000000000'}]}, {'Data': [{'ScalarValue': 'ETH'}, {'ScalarValue': 'open'}, {'ScalarValue': '1739.47'}, {'ScalarValue': '2021-03-13 00:30:00.000000000'}]}]
        returns: dict of arrays: 
        """
        last_candles = {}
        for row in response["Rows"]:
            coin_name = row["Data"][0]["ScalarValue"]
            measure_name = row["Data"][1]["ScalarValue"] # e.g: close, high, volume, rsi
            measure_value = row["Data"][2]["ScalarValue"]
        
            if coin_name not in last_candles:
                last_candles[coin_name] = {}
            if measure_name not in last_candles[coin_name]:
                last_candles[coin_name][measure_name] = []

            last_candles[coin_name][measure_name].append(float(measure_value))

        return last_candles

    def build_records(self, params:Dict, coin:str, currency:str, exchange:str):
        """
        params = {
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
        result = {}
        # USDT BTC vs currency dinamik olsun
        result = {
            "dimension": [
                {"Name": "ticker", "Value": f"{coin}_{currency}"},
                {"Name": "interval", "Value": params["k"]["i"]},
                {"Name": "exchange", "Value": exchange},
            ],
            "measure": {}
        }

        # main attributes
        result["measure"]["open"] = str(params["k"]["o"])
        result["measure"]["close"] = str(params["k"]["c"])
        result["measure"]["high"] = str(params["k"]["h"])
        result["measure"]["low"] = str(params["k"]["l"])
        result["measure"]["volume"] = str(params["k"]["v"])
        result["measure"]["number_of_trades"] = str(params["k"]["n"])
        result["time"] = str(params["k"]["t"])[:-3] # opening time

        try:
            last_candles = self.get_last_candles(time_period=self.__number_of_candles_time_period(params), interval=params["k"]["i"])
            last_candles_coin = Sequence(last_candles[params["s"]])
            # indicators
            result["measure"]["rsi"] = "{:.2f}".format(last_candles_coin.rsi()[-1])
            result["measure"]["cci"] = "{:.2f}".format(last_candles_coin.cci()[-1])
            result["measure"]["mfi"] = "{:.2f}".format(last_candles_coin.mfi()[-1])
            result["measure"]["atr"] = "{:.2f}".format(last_candles_coin.atr()[-1])
            result["measure"]["kama"] = "{:.2f}".format(last_candles_coin.kama()[-1])
            result["measure"]["inverse_fisher_rsi_normalized"] = "{:.2f}".format(last_candles_coin.inverse_fisher_transform(indicator="rsi", normalized=True)[-1])
            result["measure"]["inverse_fisher_cci_normalized"] = "{:.2f}".format(last_candles_coin.inverse_fisher_transform(indicator="cci", period=8, normalized=True)[-1])
            result["measure"]["inverse_fisher_mfi_normalized"] = "{:.2f}".format(last_candles_coin.inverse_fisher_transform(indicator="mfi", period=13, normalized=True)[-1])
            macd = last_candles_coin.macd()
            result["measure"]["macd"] = "{:.2f}".format(macd[0][-1])
            result["measure"]["macd_signal"] = "{:.2f}".format(macd[1][-1])
            result["measure"]["macd_diff"] = "{:.2f}".format(macd[2][-1])
            ichimoku = last_candles_coin.ichimoku()
            result["measure"]["ichimoku_senkou_span_a"] = "{:.2f}".format(ichimoku[0][-1])
            result["measure"]["ichimoku_senkou_span_b"] = "{:.2f}".format(ichimoku[1][-1])
            result["measure"]["ichimoku_kijun_sen"] = "{:.2f}".format(ichimoku[2][-1])
            result["measure"]["ichimoku_tenkan_sen"] = "{:.2f}".format(ichimoku[3][-1])
        except Exception as err:
            logger.error(f"There is no record in database for coin: {params['s']}, time_period: {self.__number_of_candles_time_period(params)}, interval: {params['k']['i']} Increase time interval. KeyError:{err}")
                
        records = []
        for measure_key in result["measure"]:
            records.append({
                "Dimensions": result["dimension"], 
                "MeasureName": measure_key, 
                "MeasureValue": result["measure"][measure_key], 
                "Time": result["time"], 
                "TimeUnit": "SECONDS"
            })

        return records

    def __number_of_candles_time_period(self, params, how_many_candle=26):
        return str(int(params["k"]["i"][:-1]) * how_many_candle)+params["k"]["i"][-1]

    def insert(self, records):
        cls = self.__class__

        for records in self.chunks(records, 100):
            try:
                response = cls.write_client.write_records(
                    DatabaseName=self.database,
                    TableName=self.table,
                    Records=records
                )
                if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    print(f"Error on insertion. Error code: {response['ResponseMetadata']['HTTPStatusCode']}. Error: {response}")

                logger.info(response) # This only prints if insert was succesfull
                logger.info(records) # This only prints if insert was succesfull
            except cls.write_client.exceptions.RejectedRecordsException as err:
                logger.error({'exception': err})
                for rejected in err.response['RejectedRecords']:
                    logger.error({
                        'reason': rejected['Reason'], 
                        'rejected_record': records[rejected['RecordIndex']]
                    })

    @staticmethod
    def chunks(records, chunk_size):
        if len(records) < chunk_size:
            return [records]

        return [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
