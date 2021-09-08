from datetime import datetime
import boto3
import botocore
from botocore.config import Config
from decimal import Decimal

#event are SQS messages
event = {
    "Records":[
        {
            "messageId":"f76eab5f-b946-42e4-ac28-759ec8f6b1fb",
            "receiptHandle":"AQEBCJDWEAtDCl6gcsa7Z8TLN7PNnfiHiXljLrY6jzA6iFTbOX7Iy13TDo6y6MRP4voPF6JrgH3i3+VfHaXeww5u5rmhMW6Ez3Yo8UxgflBGkxjj288Hi9fEe4wFt9/N0RrI2iKASlYKpe2Fn2+xgKoZg/VpWRXeTFWI/vBJZ+k2LxbLCuHOV0/IUZdYW14NESzxZ7dYMVLzJeHFpdYzt+UkDEdf/q6vjEcb4/qi0EU9mxMCFxBY2EaqXaQ/ATBoAIVCgfsgUkNTq8w18m1Kh6IgTg==",
            "body":"DOT_USDT_5m",
            "attributes":{
                "ApproximateReceiveCount":"1",
                "SentTimestamp":"1628791206352",
                "SequenceNumber":"18863714622535663872",
                "MessageGroupId":"test",
                "SenderId":"AIDAXJMSEDOFCFHOCLFRG",
                "MessageDeduplicationId":"202260be-fb97-11eb-b10f-b827eb01a934",
                "ApproximateFirstReceiveTimestamp":"1628791206352"
            },
            "messageAttributes":{
                "volume":{
                "stringValue":"3010.97920000",
                "stringListValues":[
                    
                ],
                "binaryListValues":[
                    
                ],
                "dataType":"String"
                },
                "high":{
                "stringValue":"379.16000000",
                "stringListValues":[
                    
                ],
                "binaryListValues":[
                    
                ],
                "dataType":"String"
                },
                "number_of_trades":{
                "stringValue":"2591",
                "stringListValues":[
                    
                ],
                "binaryListValues":[
                    
                ],
                "dataType":"String"
                },
                "low":{
                "stringValue":"377.36000000",
                "stringListValues":[
                    
                ],
                "binaryListValues":[
                    
                ],
                "dataType":"String"
                },
                "currency":{
                "stringValue":"USDT",
                "stringListValues":[
                    
                ],
                "binaryListValues":[
                    
                ],
                "dataType":"String"
                },
                "interval":{
                "stringValue":"5m",
                "stringListValues":[
                    
                ],
                "binaryListValues":[
                    
                ],
                "dataType":"String"
                },
                "time":{
                "stringValue":"1630528199",
                "stringListValues":[
                    
                ],
                "binaryListValues":[
                    
                ],
                "dataType":"String"
                },
                "close":{
                "stringValue":"384.15000000",
                "stringListValues":[
                    
                ],
                "binaryListValues":[
                    
                ],
                "dataType":"String"
                },
                "open":{
                "stringValue":"379.13000000",
                "stringListValues":[
                    
                ],
                "binaryListValues":[
                    
                ],
                "dataType":"String"
                },
                "coin":{
                "stringValue":"BNB",
                "stringListValues":[
                    
                ],
                "binaryListValues":[
                    
                ],
                "dataType":"String"
                }
            },
            "md5OfMessageAttributes":"922aa8eb0f7c56499b0ef84456e74778",
            "md5OfBody":"5d8e42b01eda1c064a516bedd3167ad4",
            "eventSource":"aws:sqs",
            "eventSourceARN":"arn:aws:sqs:eu-west-1:501207014282:ticker-ohlcv.fifo",
            "awsRegion":"eu-west-1"
        }
    ]
}

TTL = {"5m": 120*24*60*60, "15m": 120*24*60*60*3, "1h": 120*24*60*60*12, "4h": 120*24*60*60*12*4, "8h": 120*24*60*60*12*8, "1d": 120*24*60*60*12*24}

class Metrics:
    def __init__(self):
        resource = boto3.resource(
            "dynamodb", 
            config=Config(read_timeout=585, connect_timeout=585)
        )
        self.table_name = "metrics"
        self.table = resource.Table(self.table_name)

    def insert(self, event):
        timestamp = int(event["Records"][0]["messageAttributes"]["time"]["stringValue"])
        response = self.table.put_item(
            Item={
                'ticker_interval': event["Records"][0]["body"],
                'time': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                'open': Decimal(event["Records"][0]["messageAttributes"]["open"]["stringValue"]),
                'high': Decimal(event["Records"][0]["messageAttributes"]["high"]["stringValue"]),
                'low': Decimal(event["Records"][0]["messageAttributes"]["low"]["stringValue"]),
                'close': Decimal(event["Records"][0]["messageAttributes"]["close"]["stringValue"]),
                'volume': Decimal(event["Records"][0]["messageAttributes"]["volume"]["stringValue"]),
                'number_of_trades': int(event["Records"][0]["messageAttributes"]["number_of_trades"]["stringValue"]),
                'TTL': timestamp + TTL[event["Records"][0]["messageAttributes"]["interval"]["stringValue"]]
            }
        )
        return response



class Summary:
    def __init__(self, event):
        self.table_name = "metrics_summary"
        resource = boto3.resource("dynamodb", config=Config(read_timeout=585, connect_timeout=585))
        self.table = resource.Table(self.table_name)
        self.event = event
        self.ticker, self.interval_metric = self.get_ticker_interval()

    def get_ticker_interval(self):
        message_body = self.event["Records"][0]["body"]
        ticker, interval = message_body.rsplit("_", 1)
        """
        Todo: Add a loop to insert other metric types (open, high, low, volume, number_of_trades)
        """
        interval_metric = interval + "_" + "close"
        return ticker, interval_metric

    def insert(self):
        try:
            self.table.put_item(
                Item={"ticker": self.ticker, "interval_metric": self.interval_metric},
                ConditionExpression="attribute_not_exists(ticker) AND attribute_not_exists(interval_metric)"
            )
        except botocore.exceptions.ClientError as e:
            # Ignore the ConditionalCheckFailedException, bubble up
            # other exceptions.
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise

def lambda_handler(event, context):
    metrics = Metrics()
    summary = Summary(event)
    metrics.insert(event)
    summary.insert()

# table = Metrics()
# item = Item(event)
# import pdb
# pdb.set_trace()