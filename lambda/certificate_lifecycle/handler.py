import os
import time
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
iot = boto3.client("iot")

DEVICE_METADATA_TABLE = os.environ["DEVICE_METADATA_TABLE"]
device_table = dynamodb.Table(DEVICE_METADATA_TABLE)

BUCKET_PREFIXES = ["TRIAL#", "ACTIVE#"]
BUCKET_PREFIX_EXPIRED = "EXPIRED#"


def _bucket_for_now(prefix: str, now: int) -> str:
    return f"{prefix}{datetime.fromtimestamp(now, tz=timezone.utc):%Y%m%d%H}"


def _expired_bucket(now: int) -> str:
    return f"{BUCKET_PREFIX_EXPIRED}{datetime.fromtimestamp(now, tz=timezone.utc):%Y%m%d%H}"


def main(event, context):
    now = int(time.time())
    expired_devices = []

    # --- Consultar buckets exactos de esta hora ---
    LOOKBACK_HOURS = 12
   
    for prefix in BUCKET_PREFIXES:
        for offset in range(LOOKBACK_HOURS):
            ts = now - offset * 3600
            bucket = _bucket_for_now(prefix, ts)
            last_evaluated_key = None

            while True:
                try:
                    response = device_table.query(
                        IndexName="ByLifecycleBucket",
                        KeyConditionExpression="lifecycleBucket = :bucket AND expiresAt <= :now",
                        ExpressionAttributeValues={
                            ":bucket": bucket,
                            ":now": now,
                        },
                        ProjectionExpression="thingName, certificateId, lifecycleStatus",
                        ExclusiveStartKey=last_evaluated_key if last_evaluated_key else None,
                    )

                    expired_devices.extend(response.get("Items", []))
                    last_evaluated_key = response.get("LastEvaluatedKey")

                    if not last_evaluated_key:
                        break

                except ClientError as e:
                    print(f"Error querying bucket {bucket}: {e}")
                    break

    # --- Procesar expiraciones ---
    processed = 0
    for device in expired_devices:
        if device.get("lifecycleStatus") not in ("TRIAL", "ACTIVE"):
            continue

        thing_name = device["thingName"]
        cert_id = device["certificateId"]

        try:
            device_table.update_item(
                Key={"thingName": thing_name},
                UpdateExpression="""
                    SET lifecycleStatus = :expired,
                        lifecycleBucket = :expired_bucket,
                        expiredAt = :now
                """,
                ConditionExpression="lifecycleStatus = :trial OR lifecycleStatus = :active",
                ExpressionAttributeValues={
                    ":expired": "EXPIRED",
                    ":expired_bucket": _expired_bucket(now),
                    ":now": now,
                    ":trial": "TRIAL",
                    ":active": "ACTIVE",
                },
            )

            try:
                cert_desc = iot.describe_certificate(
                    certificateId=cert_id
                )["certificateDescription"]

                if cert_desc["status"] == "ACTIVE":
                    iot.update_certificate(
                        certificateId=cert_id,
                        newStatus="INACTIVE"
                    )
            except ClientError as cert_error:
                print(f"Could not update certificate {cert_id}: {cert_error}")

            processed += 1

        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                print(f"Error expiring device {thing_name}: {e}")

    print(f"Certificate lifecycle run completed: {processed} devices expired.")
    return {"status": "ok", "processed": processed}
