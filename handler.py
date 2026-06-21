import json
import os
import boto3
import urllib
import csv
import codecs
import time

from decimal import Decimal
from datetime import datetime
from boto3.dynamodb.conditions import Key


def get_dynamodb():
    return boto3.resource('dynamodb', region_name='ap-southeast-1')

def get_s3():
    return boto3.client('s3', region_name='us-east-2')


class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
      return str(obj)
    return json.JSONEncoder.default(self, obj)


def get_all_products(event, context):
    table_name = "products-table-paul"
    dynamodb = get_dynamodb()
    table = dynamodb.Table(table_name)

    return_body = {}
    return_body["items"] = table.scan().get('Items')
    return_body["status"] = "success"

    response = {"statusCode": 200, "body": json.dumps(return_body, cls=DecimalEncoder)}
    return response


def create_one_product(event, context):
    body = json.loads(event["body"], parse_float=Decimal)
    table_name = "products-table-paul"
    dynamodb = get_dynamodb()
    table = dynamodb.Table(table_name)

    table.put_item(Item=body)

    response = {"statusCode": 200, "body": json.dumps(body, cls=DecimalEncoder)}

    sqs = boto3.resource('sqs', region_name='ap-southeast-1')
    queue = sqs.get_queue_by_name(QueueName='products-sqs-paul')
    queue.send_message(MessageBody=json.dumps(body, cls=DecimalEncoder))

    logs_client = boto3.client('logs', region_name='ap-southeast-1')
    LOG_GROUP = "ProductsEventLogGroup-paul"
    LOG_STREAM = "ProductEventStream-paul"

    try:
        logs_client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

    log_event = {
        "event": "product_created",
        "pid": body.get("product_id"),
        "data": body
    }

    logs_client.put_log_events(
        logGroupName=LOG_GROUP,
        logStreamName=LOG_STREAM,
        logEvents=[
            {
                'timestamp': int(time.time() * 1000),
                'message': json.dumps(log_event, cls=DecimalEncoder)
            }
        ]
    )
    print("Product creation event logged in CloudWatch.")

    return response


def get_one_product(event, context):
    path_params = event.get("pathParameters", {})
    product_id = path_params.get("product_id")

    dynamodb = get_dynamodb()

    # A. Fetch metadata from your main products table
    product_table = dynamodb.Table('products-table-paul')
    product_data = product_table.get_item(Key={'product_id': product_id}).get('Item')

    if not product_data:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": "Product not found po"})
        }

    # B. Query all matching stock records from your new inventory table
    inventory_table = dynamodb.Table('product-inventory-paul')
    inventory_history = inventory_table.query(
        KeyConditionExpression=Key('product_id').eq(product_id)
    ).get('Items', [])

    # C. Calculate total stock dynamically!
    total_stock = sum(item.get('quantity', 0) for item in inventory_history)

    # D. Inject calculation results into the response payload
    product_data["total_stocks"] = total_stock
    product_data["inventory_logs"] = inventory_history

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "success", "item": product_data}, cls=DecimalEncoder)
    }


def delete_one_product(event, context):
    path_params = event.get("pathParameters", {})
    product_id = path_params.get("product_id")

    if not product_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing product_id in URL path"})
        }

    table_name = "products-table-paul"
    dynamodb = get_dynamodb()
    table = dynamodb.Table(table_name)

    table.delete_item(Key={'product_id': product_id})

    response = {
        "statusCode": 200,
        "body": json.dumps({
            "status": "success",
            "message": f"Product {product_id} has been successfully deleted po!"
        })
    }
    return response


def update_product(event, context):
    path_params = event.get("pathParameters", {})
    product_id = path_params.get("product_id")
    body = json.loads(event["body"], parse_float=Decimal)

    table_name = "products-table-paul"
    dynamodb = get_dynamodb()
    table = dynamodb.Table(table_name)

    table.update_item(
        Key={'product_id': product_id},
        UpdateExpression="set quantity = :q",
        ExpressionAttributeValues={
            ':q': str(body.get('quantity'))
        }
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "success", "message": f"Product {product_id} updated successfully po!"})
    }


def add_stocks_to_product(event, context):
    path_params = event.get("pathParameters", {})
    product_id = path_params.get("product_id")
    body = json.loads(event["body"], parse_float=Decimal)

    current_time = datetime.utcnow().isoformat()

    dynamodb = get_dynamodb()
    inventory_table = dynamodb.Table('product-inventory-paul')

    inventory_item = {
        "product_id": product_id,
        "datetime": current_time,
        "quantity": Decimal(str(body.get("quantity"))),
        "remarks": body.get("remarks", "No remarks po")
    }

    inventory_table.put_item(Item=inventory_item)

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "success", "added_entry": inventory_item}, cls=DecimalEncoder)
    }


def batch_create_products(event, context):
    print("csv file uploaded trigger")
    print(event)

    print("Extract file location from event payload")
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    localFilename = f'/tmp/{key}'
    s3_client = get_s3()
    print("downloaded file to /tmp folder")
    s3_client.download_file(bucket, key, localFilename)

    print("reading CSV file and looping it over...")

    with open(localFilename, 'r') as f:
        csv_reader = csv.DictReader(f)
        table_name = "products-table-paul"
        dynamodb = get_dynamodb()
        table = dynamodb.Table(table_name)
        for row in csv_reader:
            table.put_item(Item=row)

    print("All done!")
    return {}
