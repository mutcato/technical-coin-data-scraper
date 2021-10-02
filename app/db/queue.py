from typing import Dict
import settings
import boto3
from botocore.config import Config
import uuid

logger = settings.logging.getLogger()

class SQS:
    client = boto3.client(
        "sqs", 
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY, 
        region_name=settings.AWS_REGION_NAME,
        config=Config(read_timeout=585, connect_timeout=585)
    )

    def create_message_attributes(self, ticker)->Dict:
        attributes = {}

        attributes["coin"] = {"StringValue": ticker.coin, 'DataType': 'String'}
        attributes["currency"] = {"StringValue": ticker.currency, 'DataType': 'String'}
        attributes["interval"] = {"StringValue": ticker.interval, 'DataType': 'String'}
        for measure_key, measure_value in ticker.measures.items():
            attributes[measure_key] = {"StringValue": str(measure_value), 'DataType': 'String'}
        attributes["time"] = {"StringValue": str(ticker.time), 'DataType': 'String'}

        return attributes

    def insert(self, ticker:Dict, queue_name:str=settings.OHLCV_QUEUE):
        queue_url = self.client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        try:
            response = self.client.send_message(
                QueueUrl=queue_url,
                MessageBody=f"{ticker.coin}_{ticker.currency}_{ticker.interval}", 
                MessageAttributes=self.create_message_attributes(ticker) 
            )
            logger.info(f"SQS response INFO: {response}")
        except Exception as e:
            logger.error(f"SQS error: {e}")
        return response