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
        activation_code = str(uuid4().hex)

        # Crear Thing
        iot.create_thing(
            thingName=thing_name,
            thingTypeName="Gateway",
            attributePayload={
                "attributes": {
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

        # Adjuntar policy IoT al certificado
        try:
            iot.attach_policy(
                policyName="GatewayBasePolicy",
                target=cert_arn
            )
        except Exception:
            iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
            raise

        iot.attach_thing_principal(
        thingName=thing_name,
        principal=cert_arn
        )

        # Activar dispositivo
        try:
            table.put_item(
                Item={
                    "thingName": thing_name,
                    "userId": None,
                    "displayName": display_name,
                    "role": "Gateway",

                    "status": "active",
                    "lifecycleStatus": "ACTIVE",  # ACTIVE | EXPIRED | REVOKED

                    "certificateArn": cert_arn,
                    "certificateId": cert_id,

                    "createdAt": now,
                    "lastRenewalDate": now,
                    "expiresAt": expires_at,
                },
                ConditionExpression="attribute_not_exists(thingName)"
            )
        except Exception:
            iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
            raise

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
