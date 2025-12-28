import json
import boto3
import os
import time
from uuid import uuid4
from boto3.dynamodb.conditions import Key

iot = boto3.client("iot")
dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["TABLE_NAME"]
DEFAULT_EXPIRATION_SECONDS = int(
    os.environ.get("DEFAULT_EXPIRATION_SECONDS", 30 * 24 * 3600)
)

table = dynamodb.Table(TABLE_NAME)


def main(event, context):
    try:
        now = int(time.time())

        # Inputs obligatorios
        user_id = event.get("userId")
        display_name = event.get("displayName", "Gateway")

        if not isinstance(user_id, str) or not user_id or len(user_id) > 64:
            return _error("invalid userId")

        # ¿Existe ya el usuario?
        try:
            resp = table.query(
                IndexName="ByUser",
                KeyConditionExpression=Key("userId").eq(user_id),
                Limit=1
            )
            is_new_user = resp["Count"] == 0
        except Exception:
            is_new_user = True

        # Expiración
        plan_days = event.get("planDays")

        if plan_days is not None:
            plan_days = int(plan_days)
            if plan_days <= 0 or plan_days > 3650:
                return _error("planDays must be between 1 and 3650")
            expires_at = now + plan_days * 86400
        else:
            expires_at = now + DEFAULT_EXPIRATION_SECONDS

        # thingName único
        thing_name = f"gw_{uuid4().hex}"

        # Guardar metadata
        table.put_item(
            Item={
                "thingName": thing_name,
                "userId": user_id,
                "displayName": display_name,
                "role": "Gateway",

                "status": "provisioning",

                "createdAt": now,
                "lastRenewalDate": now,
                "expiresAt": expires_at,
            },
            ConditionExpression="attribute_not_exists(thingName)"
        )


        # Crear Thing
        iot.create_thing(
            thingName=thing_name,
            thingTypeName="Gateway",
            attributePayload={
                "attributes": {
                    "userId": user_id,
                    "role": "Gateway",
                    "displayName": display_name,
                    "createdAt": str(now),
                }
            }
        )

        # Certificado
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]

        # Asociar certificado al Thing
        iot.attach_thing_principal(
            thingName=thing_name,
            principal=cert_arn
        )

        # Activar dispositivo
        table.update_item(
            Key={"thingName": thing_name},
            UpdateExpression="""
                SET #s = :active,
                    certificateArn = :carn,
                    certificateId = :cid
            """,
            ExpressionAttributeNames={
                "#s": "status",
            },
            ExpressionAttributeValues={
                ":active": "active",
                ":carn": cert_arn,
                ":cid": cert_id,
            }
        )

        # Response
        return {
            "status": "ok",
            "thingName": thing_name,
            "displayName": display_name,
            "certificatePem": cert["certificatePem"],
            "privateKey": cert["keyPair"]["PrivateKey"],
            "publicKey": cert["keyPair"]["PublicKey"],
            "gatewayTopic": f"gateway/{user_id}/data/telemetry",
            "isNewUser": is_new_user
        }

    except Exception as e:
        print("DeviceFactory error:", str(e))
        return _error(str(e))


def _error(msg):
    return {"status": "error", "message": msg}
