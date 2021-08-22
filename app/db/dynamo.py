import boto3
import botocore
from decimal import Decimal
import settings

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
                "stringValue":"162872699",
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

class Mertics:
    def __init__(self):
        resource = boto3.resource(
            "dynamodb", 
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY, 
            region_name=settings.AWS_REGION_NAME
        )
        self.table_name = "metrics"
        self.table = resource.Table(self.table_name)
        
    def has_item(self, item)->bool:
        response = self.table.get_item(Key={
          "ticker": item.partition_key,
          "interval_metric": item.sort_key
        })
        return "Item" in response

    def append_item_list(self, item, attribute1, attribute2):
        result = self.table.update_item(
            Key={
                'ticker': item.partition_key,
                'interval_metric': item.sort_key
            },
            UpdateExpression=f"SET {attribute1} = list_append({attribute1}, :i), {attribute2} = list_append({attribute2}, :j)",
            ExpressionAttributeValues={
                ':i': [round(Decimal(item.metric_value),2)],
                ':j': [round(Decimal(item.timestamp),2)],
            },
            ReturnValues="UPDATED_NEW"
        )
        if result['ResponseMetadata']['HTTPStatusCode'] == 200 and 'Attributes' in result:
            return result['Attributes']

    def insert_item(self, item):
        response = self.table.put_item(
            Item={
                'ticker': item.partition_key,
                'interval_metric': item.sort_key,
                'metric_values': [round(Decimal(item.metric_value),4)],
                'timestamps': [round(Decimal(item.timestamp),4)],
            }
        )
        return response

    def process_item(self, item):
        if self.has_item(item):
            print("UPDATE")
            response = self.append_item_list(item, "metric_values", "timestamps")
        else:
            print("INSERT")
            response = self.insert_item(item)

        return response

        

class Item:
    def __init__(self, event, metric:str="close"):
        self.event = event
        self.metric_name = metric
        self.partition_key, self.sort_key = self.get_partition_key(metric)
        self.metric_value = event["Records"][0]["messageAttributes"][metric]["stringValue"]
        self.timestamp = event["Records"][0]["messageAttributes"]["time"]["stringValue"]
        
    def get_partition_key(self, metric:str):
        message_body = self.event["Records"][0]["body"]
        ticker, interval = message_body.rsplit("_", 1)
        sort_key = interval + "_" + metric
        return ticker, sort_key


class Summary:
    def __init__(self):
        self.table_name = "metrics_summary"
        resource = boto3.resource("dynamodb")
        self.table = resource.Table(self.table_name)

    def insert(self, ticker, interval_metric):
        try:
            self.table.put_item(
                Item={"ticker": ticker, "interval_metric": interval_metric},
                ConditionExpression="attribute_not_exists(ticker) AND attribute_not_exists(interval_metric)"
            )
        except botocore.exceptions.ClientError as e:
            # Ignore the ConditionalCheckFailedException, bubble up
            # other exceptions.
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise


# table = Metrics()
# item = Item(event)
# import pdb
# pdb.set_trace()