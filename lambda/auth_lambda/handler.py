import json
import os
import time
import boto3

dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ.get("DEVICE_METADATA_TABLE")
if not TABLE_NAME:
    raise RuntimeError("DEVICE_METADATA_TABLE environment variable is not set")
table = dynamodb.Table(TABLE_NAME)

EXPIRATION_SECONDS = int(os.environ.get("EXPIRATION_SECONDS", 30 * 24 * 3600))

def lambda_handler(event, context):
    try:
        thing_name = None

        if isinstance(event, dict):
            thing_name = event.get("thingName") or event.get("token") or event.get("principalId")

        if not thing_name:
            return _deny("no_thing")
        if (
            not isinstance(thing_name, str)
            or len(thing_name) > 64
            or not thing_name.replace("_", "").replace("-", "").isalnum()
        ):
            return _deny("invalid_thing_name")

        resp = table.get_item(Key={"thingName": thing_name})
        item = resp.get("Item")
        if not item:
            return _deny("not_registered")

        status = item.get("status", "inactive")
        last = int(item.get("lastRenewalDate", 0))
        now = int(time.time())

        if status != "active" or (now - last) > EXPIRATION_SECONDS:
            return _deny("expired_or_inactive")

        user_id = item.get("userId", "unknown")


        # Misma política del Device Factory
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": ["iot:Connect"],
                    "Effect": "Allow",
                    "Resource": [
                        f"arn:aws:iot:*:*:client/${{iot:ClientId}}"
                    ]
                },
                {
                    "Action": ["iot:Publish"],
                    "Effect": "Allow",
                    "Resource": [
                        f"arn:aws:iot:*:*:topic/gateway/{user_id}/data/telemetry"
                    ]
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
        return _deny("error")

def _deny(reason):
    return {
        "isAuthenticated": False,
        "principalId": "anonymous",
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": ["iot:Connect", "iot:Publish", "iot:Subscribe", "iot:Receive"],
                    "Effect": "Deny",
                    "Resource": ["*"]
                }
            ]
        },
        "context": {"reason": reason}
    }

