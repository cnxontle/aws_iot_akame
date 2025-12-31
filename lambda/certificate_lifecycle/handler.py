import os
import time
import boto3
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
iot = boto3.client("iot")

DEVICE_METADATA_TABLE = os.environ["DEVICE_METADATA_TABLE"]
device_table = dynamodb.Table(DEVICE_METADATA_TABLE)

BUCKET_PREFIX_TRIAL = "TRIAL#"
BUCKET_PREFIX_EXPIRED = "EXPIRED#"


def _expired_bucket(now: int) -> str:
    return f"{BUCKET_PREFIX_EXPIRED}{datetime.fromtimestamp(now, tz=timezone.utc):%Y%m%d%H}"


def main(event, context):
    now = int(time.time())

    # Generar lista de buckets TRIAL# de las últimas 72 horas (más que suficiente para trials de 3 días)
    current_time = datetime.fromtimestamp(now, tz=timezone.utc)
    buckets_to_check = []
    for hour_offset in range(72):
        bucket_time = current_time - timedelta(hours=hour_offset)
        bucket = f"{BUCKET_PREFIX_TRIAL}{bucket_time:%Y%m%d%H}"
        buckets_to_check.append(bucket)

    expired_devices = []

    # Query cada bucket posible buscando dispositivos expirados
    for bucket in buckets_to_check:
        last_evaluated_key = None
        while True:
            query_kwargs = {
                "IndexName": "ByLifecycleBucket",
                "KeyConditionExpression": "lifecycleBucket = :bucket AND expiresAt <= :now",
                "ExpressionAttributeValues": {
                    ":bucket": bucket,
                    ":now": now
                },
                "ProjectionExpression": "thingName, certificateId, lifecycleStatus"
            }
            if last_evaluated_key:
                query_kwargs["ExclusiveStartKey"] = last_evaluated_key

            try:
                response = device_table.query(**query_kwargs)
                items = response.get("Items", [])
                expired_devices.extend(items)

                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break
            except ClientError as e:
                print(f"Error querying bucket {bucket}: {e}")
                break

    # Procesar dispositivos expirados
    processed = 0
    for device in expired_devices:
        if device.get("lifecycleStatus") != "TRIAL":
            continue

        thing_name = device["thingName"]
        cert_id = device["certificateId"]

        try:
            # Marcar como expirado en DynamoDB
            device_table.update_item(
                Key={"thingName": thing_name},
                UpdateExpression="""
                    SET lifecycleStatus = :expired,
                        lifecycleBucket = :expired_bucket,
                        expiredAt = :now
                """,
                ConditionExpression="lifecycleStatus = :trial",
                ExpressionAttributeValues={
                    ":expired": "EXPIRED",
                    ":expired_bucket": _expired_bucket(now),
                    ":now": now,
                    ":trial": "TRIAL"
                }
            )

            # Inactivar certificado si está activo
            try:
                cert_desc = iot.describe_certificate(certificateId=cert_id)["certificateDescription"]
                if cert_desc["status"] == "ACTIVE":
                    iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
            except ClientError as cert_error:
                print(f"Could not update certificate {cert_id}: {cert_error}")

            processed += 1

        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                print(f"Error expiring device {thing_name}: {e}")

    print(f"Certificate lifecycle run completed: {processed} devices expired.")
    return {"status": "ok", "processed": processed}