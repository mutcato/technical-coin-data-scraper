from typing import Dict
import settings
import boto3
from botocore.config import Config
import uuid

logger = settings.logging.getLogger()

class SQS:
    write_client = boto3.resource(
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
        queue = self.write_client.get_queue_by_name(QueueName=queue_name)
        try:
            response = queue.send_message(
                MessageBody=f"{ticker.coin}_{ticker.currency}_{ticker.interval}", 
                MessageAttributes=self.create_message_attributes(ticker), 
                MessageGroupId="test",
                MessageDeduplicationId=str(uuid.uuid1())
            )
        except Exception as e:
            logger.error("SQS error: "+e)
        return response