import os
import time
import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb")
iot = boto3.client("iot")

ACTIVATION_CODE_TABLE = os.environ["ACTIVATION_CODE_TABLE"]
DEVICE_METADATA_TABLE = os.environ["DEVICE_METADATA_TABLE"]

activation_table = dynamodb.Table(ACTIVATION_CODE_TABLE)
device_table = dynamodb.Table(DEVICE_METADATA_TABLE)


def _bucket_for_expiry(expires_at: int) -> str:
    return f"ACTIVE#{datetime.fromtimestamp(expires_at, tz=timezone.utc):%Y%m%d%H}"


def main(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        activation_code = body.get("activationCode")
        cognito_sub = (
            event["requestContext"]
            ["authorizer"]
            ["jwt"]
            ["claims"]
            ["sub"]
        )
        display_name = body.get("displayName", "unassigned")

        if not activation_code or not cognito_sub:
            return {"status": "error", "message": "invalid input"}

        now = int(time.time())

        # Obtener activation code
        code_resp = activation_table.get_item(
            Key={"code": activation_code}
        )

        if "Item" not in code_resp:
            return {"status": "error", "message": "activation code invalid"}

        code_item = code_resp["Item"]
        thing_name = code_item["thingName"]
        plan_seconds = int(code_item["planSeconds"])
        new_expires_at = now + plan_seconds

        # Obtener certificateId
        device_resp = device_table.get_item(
            Key={"thingName": thing_name},
            ProjectionExpression="certificateId, lifecycleStatus, userId"
        )

        if "Item" not in device_resp:
            return {"status": "error", "message": "device not found"}

        cert_id = device_resp["Item"]["certificateId"]

        # Actualizar metadata (paso CRÍTICO)
        try:
            device_table.update_item(
                Key={"thingName": thing_name},
                UpdateExpression="""
                    SET userId = :uid,
                        activatedBy = if_not_exists(activatedBy, :uid),
                        lastRenewalDate = :now,
                        expiresAt = :exp,
                        lifecycleStatus = :active,
                        lifecycleBucket = :bucket,
                        displayName = :dn
                """,
                ConditionExpression="""
                    lifecycleStatus IN (:trial, :expired)
                    AND (attribute_not_exists(userId) OR userId = :unassigned_val)
                """,
                ExpressionAttributeValues={
                    ":uid": cognito_sub,
                    ":now": now,
                    ":exp": new_expires_at,
                    ":active": "ACTIVE",
                    ":trial": "TRIAL",
                    ":expired": "EXPIRED",
                    ":unassigned_val": "unassigned",
                    ":bucket": _bucket_for_expiry(new_expires_at),
                    ":dn": display_name
                },
            )

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return {
                    "status": "error",
                    "message": "device already activated or owned by another user",
                }
            raise

        # Reactivar certificado si estaba inactivo
        try:
            cert_desc = iot.describe_certificate(
                certificateId=cert_id
            )["certificateDescription"]

            if cert_desc["status"] != "ACTIVE":
                iot.update_certificate(
                    certificateId=cert_id,
                    newStatus="ACTIVE"
                )
        except ClientError as cert_error:
            print(f"Certificate update failed {cert_id}: {cert_error}")
            # NO abortamos: metadata ya es la fuente de verdad

        # Actualizar atributo userId en el Thing
        try:
            iot.update_thing(
                thingName=thing_name,
                attributePayload={
                    "attributes": {
                        "userId": cognito_sub,
                        "displayName": display_name
                        },
                    "merge": True,
                },
            )
        except ClientError as thing_error:
            print(f"Thing update failed {thing_name}: {thing_error}")

        # Borrar activation code (ÚLTIMO PASO)
        try:
            activation_table.delete_item(
                Key={"code": activation_code},
                ConditionExpression="attribute_exists(thingName) AND thingName = :tn",
                ExpressionAttributeValues={":tn": thing_name},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "status": "ok",
                "thingName": thing_name,
                "activatedAt": now,
                "expiresAt": new_expires_at,
                "userId": cognito_sub
            })
        }

    except Exception as e:
        print("ConsumeActivationCode error:", str(e))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "status": "error",
                "message": "internal error"
            })
        }
