import json
import os
import time
import boto3

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("DEVICE_METADATA_TABLE")

if not TABLE_NAME:
    raise RuntimeError("DEVICE_METADATA_TABLE environment variable is not set")

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    # Log completo del evento para depuración
    print("Authorizer Event:", json.dumps(event))

    thing_name = None
    
    try:
        now = int(time.time())

        thing_name = event.get("principalId") or event.get("authorizationToken")

        if not thing_name:
            return _deny("no_thing")

        # Validación básica del ClientId
        if (
            not isinstance(thing_name, str)
            or len(thing_name) > 64
            or not thing_name.replace("_", "").replace("-", "").isalnum()
        ):
            return _deny("invalid_thing_name", thing_name)

        # Buscar en DynamoDB
        resp = table.get_item(Key={"thingName": thing_name})
        item = resp.get("Item")

        if not item:
            return _deny("not_registered", thing_name)

        status = item.get("status", "inactive")
        expires_at = int(item.get("expiresAt", 0))
        is_expired = now > expires_at

        if status != "active" or is_expired:
            return _deny(f"inactive_or_expired: {status} (Expires: {expires_at})", thing_name)

        user_id = item.get("userId", "unknown")

        # Política de autorización MQTT
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": ["iot:Connect"],
                    "Effect": "Allow",
                    "Resource": [f"arn:aws:iot:*:*:client/${{iot:ClientId}}"]
                },
                {
                    "Action": ["iot:Publish"],
                    "Effect": "Allow",
                    "Resource": [f"arn:aws:iot:*:*:topic/gateway/{user_id}/data/telemetry"]
                },
                {
                    "Action": ["iot:Subscribe", "iot:Receive"],
                    "Effect": "Allow",
                    "Resource": [
                        f"arn:aws:iot:*:*:topicfilter/gateway/{user_id}/command/#",
                        f"arn:aws:iot:*:*:topic/gateway/{user_id}/command/#"
                    ]
                }
            ]
        }

        return {
            "isAuthenticated": True,
            "principalId": thing_name,
            "policyDocument": policy_doc,
            "context": {"userId": user_id}
        }

    except Exception as e:
        print(json.dumps({
            "error": str(e),
            "thing": thing_name,
            "source": "iot_authorizer"
        }))
        return _deny("error", thing_name)


def _deny(reason, principal_id="anonymous"):
    if principal_id is None:
        principal_id = "anonymous"
    return {
        "isAuthenticated": False,
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": ["iot:Connect"],
                    "Effect": "Deny",
                    "Resource": ["*"]
                }
            ]
        },
        "context": {"reason": reason}
    }
