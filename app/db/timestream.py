from datetime import datetime
from typing import Dict, List
import settings
import boto3


logger = settings.logging.getLogger()


class TimeStream:
    write_client = boto3.client("timestream-write")
    query_client = boto3.client("timestream-query")

    insertion_limit = 100  # you can insert this many records at once

    def __init__(
        self,
        database=settings.TIMESTREAM_DATABASE_TEST,
        table=settings.TIMESTREAM_TABLE_TEST,
    ):
        self.database = database
        self.table = table

    def build_records(self, stream) -> List:
        measure_names = ["open", "high", "low", "close", "volume", "number_of_trades"]
        records = []
        for item_verbose in stream["Records"]:
            if item_verbose["eventName"] == "INSERT":
                ticker, interval = tuple(
                    item_verbose["dynamodb"]["Keys"]["ticker_interval"]["S"].rsplit(
                        "_", 1
                    )
                )
                timestamp = int(
                    datetime.strptime(
                        item_verbose["dynamodb"]["Keys"]["time"]["S"],
                        "%Y-%m-%d %H:%M:%S",
                    ).timestamp()
                )
                item = item_verbose["dynamodb"]["NewImage"]
                for measure_name in measure_names:
                    records.append(
                        {
                            "Dimensions": [
                                {"Name": "ticker", "Value": ticker},
                                {"Name": "interval", "Value": interval},
                                {"Name": "exchange", "Value": "binance"},
                            ],
                            "MeasureName": measure_name,
                            "MeasureValue": str(item[measure_name]["N"]),
                            "Time": str(timestamp),
                            "TimeUnit": "SECONDS",
                        }
                    )

        return records

    def insert(self, event):
        cls = self.__class__

        records = self.build_records(stream=event)
        if len(records) > 0:
            for batch in self.chunks(records, self.insertion_limit):
                try:
                    response = cls.write_client.write_records(
                        DatabaseName=self.database, TableName=self.table, Records=batch
                    )
                    if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                        print(
                            f"Error on insertion. Search this log. Error code: {response['ResponseMetadata']['HTTPStatusCode']}. Error: {response}"
                        )

                    logger.info(batch)  # This only prints if insert was succesfull
                except cls.write_client.exceptions.RejectedRecordsException as err:
                    logger.error({"exception": err})
                    for rejected in err.response["RejectedRecords"]:
                        logger.error(
                            {
                                "reason": rejected["Reason"],
                                "rejected_record": batch[rejected["RecordIndex"]],
                            }
                        )

    @staticmethod
    def chunks(records, chunk_size):
        if len(records) < chunk_size:
            return [records]

        return [records[i : i + chunk_size] for i in range(0, len(records), chunk_size)]


def lambda_handler(event, context):
    timestream = TimeStream()
    timestream.insert(event)
