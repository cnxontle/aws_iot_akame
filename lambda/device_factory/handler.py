import os
import time
import boto3
from uuid import uuid4
from datetime import datetime, timezone
from botocore.exceptions import ClientError

iot = boto3.client("iot")
dynamodb = boto3.resource("dynamodb")

METADATA_TABLE = os.environ["METADATA_TABLE"]
ACTIVATION_TABLE = os.environ["ACTIVATION_CODE_TABLE"]
DEFAULT_EXPIRATION_SECONDS = int(
    os.environ.get("DEFAULT_EXPIRATION_SECONDS", 3 * 24 * 3600)
)

metadata_table = dynamodb.Table(METADATA_TABLE)
activation_table = dynamodb.Table(ACTIVATION_TABLE)

BUCKET_PREFIXES = {
    "TRIAL": "TRIAL#",
    "ACTIVE": "ACTIVE#",
}


def _bucket_for_expiry(expires_at: int, lifecycle_status: str) -> str:
    prefix = BUCKET_PREFIXES[lifecycle_status]
    return f"{prefix}{datetime.fromtimestamp(expires_at, tz=timezone.utc):%Y%m%d%H}"


def _generate_activation_code() -> str:
    import secrets, string
    alphabet = string.ascii_uppercase + string.digits
    return "ACT-" + "".join(secrets.choice(alphabet) for _ in range(10))


def main(event, context):
    plan_days = None
    try:
        plan_days = int(event.get("planDays")) if event.get("planDays") else None
    except (ValueError, TypeError):
        plan_days = None


    try:
        now = int(time.time())

        if plan_days and plan_days > 0:
            plan_seconds = plan_days * 24 * 3600
        else:
            plan_seconds = DEFAULT_EXPIRATION_SECONDS

        expires_at = now + DEFAULT_EXPIRATION_SECONDS
        thing_name = f"gw_{uuid4().hex}"

        # --- Crear Thing ---
        iot.create_thing(
            thingName=thing_name,
            thingTypeName="Gateway",
            attributePayload={
                "attributes": {
                    "role": "Gateway",
                    "displayName": "unassigned",
                    "userId": "unassigned",
                    "createdAt": str(now),
                }
            },
        )

        # --- Crear certificado ---
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]

        try:
            iot.attach_policy(
                policyName="GatewayBasePolicy",
                target=cert_arn
            )
            iot.attach_thing_principal(
                thingName=thing_name,
                principal=cert_arn
            )
        except Exception:
            # Cleanup defensivo
            try: iot.detach_policy(policyName="GatewayBasePolicy", target=cert_arn)
            except: pass
            try: iot.detach_thing_principal(thingName=thing_name, principal=cert_arn)
            except: pass
            try: iot.update_certificate(certificateId=cert_id, newStatus="REVOKED")
            except: pass
            try: iot.delete_certificate(certificateId=cert_id)
            except: pass
            try: iot.delete_thing(thingName=thing_name)
            except: pass
            raise

        # --- Código de activación ---
        activation_code = None
        for _ in range(10):
            code = _generate_activation_code()
            try:
                activation_table.put_item(
                    Item={
                        "code": code,
                        "thingName": thing_name,
                        "createdAt": now,
                        "planSeconds": plan_seconds,
                    },
                    ConditionExpression="attribute_not_exists(code)",
                )
                activation_code = code
                break
            except ClientError as e:
                if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise

        if not activation_code:
            raise Exception("Could not generate unique activation code")

        lifecycle_status = "TRIAL"

        # --- Metadata ---
        metadata_table.put_item(
            Item={
                "thingName": thing_name,
                "userId": "unassigned",
                "displayName": "unassigned",
                "role": "Gateway",
                "lifecycleStatus": lifecycle_status,
                "lifecycleBucket": _bucket_for_expiry(expires_at, lifecycle_status),
                "certificateArn": cert_arn,
                "certificateId": cert_id,
                "createdAt": now,
                "lastRenewalDate": None,
                "expiredAt": None,
                "expiresAt": expires_at,
            },
            ConditionExpression="attribute_not_exists(thingName)",
        )

        return {
            "status": "ok",
            "thingName": thing_name,
            "activationCode": activation_code,
            "certificatePem": cert["certificatePem"],
            "privateKey": cert["keyPair"]["PrivateKey"],
            "publicKey": cert["keyPair"]["PublicKey"],
            "gatewayTopic": "gateway/data/telemetry/" + thing_name,
        }

    except Exception as e:
        print("DeviceFactory error:", str(e))
        return {"status": "error", "message": str(e)}
