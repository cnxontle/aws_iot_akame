import os
import time
import boto3
from uuid import uuid4
from datetime import datetime, timezone

iot = boto3.client("iot")
dynamodb = boto3.resource("dynamodb")

METADATA_TABLE = os.environ["METADATA_TABLE"]
ACTIVATION_TABLE = os.environ["ACTIVATION_CODE_TABLE"]
DEFAULT_EXPIRATION_SECONDS = int(os.environ.get("DEFAULT_EXPIRATION_SECONDS", 3 * 24 * 3600))

metadata_table = dynamodb.Table(METADATA_TABLE)
activation_table = dynamodb.Table(ACTIVATION_TABLE)
BUCKET_PREFIX = "TRIAL#"

def _bucket(now: int) -> str:
    return f"{BUCKET_PREFIX}{datetime.fromtimestamp(now, tz=timezone.utc):%Y%m%d%H}"

def _generate_activation_code() -> str:
    import secrets, string
    alphabet = string.ascii_uppercase + string.digits
    return "ACT-" + "".join(secrets.choice(alphabet) for _ in range(10))

def main(event, context):
    try:
        now = int(time.time())
        expires_at = now + DEFAULT_EXPIRATION_SECONDS

        thing_name = f"gw_{uuid4().hex}"
      

        # Crear Thing
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

        # Crear certificado
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]

        try:
            iot.attach_policy(policyName="GatewayBasePolicy", target=cert_arn)
            iot.attach_thing_principal(thingName=thing_name, principal=cert_arn)
        except Exception as attach_exc:
            # Cleanup robusto
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
            raise attach_exc



        # Guardar código de activación
        for _ in range(3):
            activation_code = _generate_activation_code()
            try:
                activation_table.put_item(
                    Item={
                        "code": activation_code,
                        "thingName": thing_name,
                        "createdAt": now,
                        "planSeconds": DEFAULT_EXPIRATION_SECONDS,
                    },
                    ConditionExpression="attribute_not_exists(code)",
                )
                break
            except Exception as e:
                if "ConditionalCheckFailedException" in str(e):
                    activation_code = None
                else:
                    raise

        if not activation_code:
            raise Exception("Could not generate unique activation code")
        
        # Guardar en metadata
        metadata_table.put_item(
            Item={
                "thingName": thing_name,
                "userId": "unassigned",
                "displayName": "unassigned",
                "role": "Gateway",
                "lifecycleStatus": "TRIAL",
                "lifecycleBucket": _bucket(now),
                "certificateArn": cert_arn,
                "certificateId": cert_id,
                "createdAt": now,
                "activatedAt": None,
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
            "gatewayTopic": "gateway/unassigned/data/telemetry",
        }

    except Exception as e:
        print("DeviceFactory error:", str(e))
        return {"status": "error", "message": str(e)}