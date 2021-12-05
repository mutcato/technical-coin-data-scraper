from decimal import Decimal
from typing import Dict
from settings import logging
import settings
import boto3


logger = logging.getLogger()

class Stream:
    write_client = boto3.client("timestream-write")
    query_client = boto3.client("timestream-query")

    insertion_limit = 100 # you can insert this many records at once

    def __init__(self):
        self.database = settings.TIMESTREAM_DATABASE
        self.table = settings.TIMESTREAM_TABLE
    

    def build_records(self, ticker):
        result = {}
        result = {
            "dimension": [
                {"Name": "ticker", "Value": f"{ticker.coin}_{ticker.currency}"},
                {"Name": "interval", "Value": ticker.interval},
                {"Name": "exchange", "Value": ticker.exchange},
            ],
            "measure": {}
        }
            
        records = []
        for measure_key in ticker.measures:
            records.append({
                "Dimensions": result["dimension"], 
                "MeasureName": measure_key, 
                "MeasureValue": str(ticker.measures[measure_key]), 
                "Time": str(ticker.time), 
                "TimeUnit": "SECONDS"
            })

        return records

    def insert(self, ticker):
        cls = self.__class__

        records = self.build_records(ticker=ticker)
        for records in self.chunks(records, 100):
            try:
                response = cls.write_client.write_records(
                    DatabaseName=self.database,
                    TableName=self.table,
                    Records=records
                )
                if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    print(f"Error on insertion. Error code: {response['ResponseMetadata']['HTTPStatusCode']}. Error: {response}")

                logger.info(records) # This only prints if insert was succesfull
            except cls.write_client.exceptions.RejectedRecordsException as err:
                logger.error({'exception': err})
                for rejected in err.response['RejectedRecords']:
                    logger.error({
                        'reason': rejected['Reason'], 
                        'rejected_record': records[rejected['RecordIndex']]
                    })

    def batch_insert(self, tickers):
        for ticker in tickers:
            self.insert(ticker)
        

    @staticmethod
    def chunks(records, chunk_size):
        if len(records) < chunk_size:
            return [records]

        return [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
