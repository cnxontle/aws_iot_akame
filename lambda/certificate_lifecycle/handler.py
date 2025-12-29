import os
import time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
iot = boto3.client("iot")

TABLE_NAME = os.environ["DEVICE_METADATA_TABLE"]
GSI_NAME = os.environ["LIFECYCLE_GSI"]
table = dynamodb.Table(TABLE_NAME)


def main(event, context):
    now = int(time.time())
    print("Lifecycle scan started")

    items = []
    exclusive_start_key = None

    # Paginación de resultados
    while True:
        query_kwargs = {
            "IndexName": GSI_NAME,
            "KeyConditionExpression":
                Key("lifecycleStatus").eq("ACTIVE") &
                Key("expiresAt").lt(now)
        }

        if exclusive_start_key:
            query_kwargs["ExclusiveStartKey"] = exclusive_start_key

        resp = table.query(**query_kwargs)
        items.extend(resp.get("Items", []))

        exclusive_start_key = resp.get("LastEvaluatedKey")
        if not exclusive_start_key:
            break
   
    # Procesar items expirados
    for item in items:
        thing_name = item["thingName"]
        cert_id = item["certificateId"]

        try:
            cert = iot.describe_certificate(
                certificateId=cert_id
            )["certificateDescription"]

            if cert["status"] == "ACTIVE":
                print(f"Deactivating cert for {thing_name}")

                iot.update_certificate(
                    certificateId=cert_id,
                    newStatus="INACTIVE"
                )

                table.update_item(
                    Key={"thingName": thing_name},
                    UpdateExpression="SET lifecycleStatus = :e, expiredAt = :now",
                    ConditionExpression="lifecycleStatus = :a",
                    ExpressionAttributeValues={
                        ":e": "EXPIRED",
                        ":a": "ACTIVE",
                        ":now": now
                    }
                )

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                pass  # ya procesado por otra ejecución
            else:
                print(f"Error processing {thing_name}: {e}")

    print("Lifecycle scan finished")
