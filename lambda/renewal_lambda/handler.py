import json
import os
import time
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ.get("DEVICE_METADATA_TABLE")
if not TABLE_NAME:
    raise RuntimeError("DEVICE_METADATA_TABLE environment variable is not set")

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    path = event.get("resource") or event.get("path")

    try:
        body = _parse_body(event)
        thing = _validate_thing_name(body)

        now = int(time.time())

        if path.endswith("/renew"):
            return _renew(thing, now)

        if path.endswith("/revoke"):
            return _revoke(thing, now)

        if path.endswith("/rehabilitate"):
            return _rehabilitate(thing, now)

        return _bad("Unknown operation")

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")

        if code == "ConditionalCheckFailedException":
            return _bad("Invalid state or thing not registered")

        print(json.dumps({
            "error": "DynamoDB error",
            "code": code,
            "thing": thing if "thing" in locals() else None,
            "source": "device_admin"
        }))
        return _bad("Storage error")

    except Exception as e:
        print(json.dumps({
            "error": str(e),
            "thing": thing if "thing" in locals() else None,
            "source": "device_admin"
        }))
        return _bad("Internal error")


# ---------- Operations ----------

def _renew(thing, now):
    table.update_item(
        Key={"thingName": thing},
        ConditionExpression="attribute_exists(thingName) AND #s = :a",
        UpdateExpression="SET lastRenewalDate = :t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":t": now,
            ":a": "active"
        }
    )

    return _ok("renewed", thing, now)


def _revoke(thing, now):
    table.update_item(
        Key={"thingName": thing},
        ConditionExpression="attribute_exists(thingName)",
        UpdateExpression="SET #s = :r, revokedAt = :t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":r": "revoked",
            ":t": now
        }
    )

    return _ok("revoked", thing, now)


def _rehabilitate(thing, now):
    table.update_item(
        Key={"thingName": thing},
        ConditionExpression="#s = :r",
        UpdateExpression="SET #s = :a, lastRenewalDate = :t, rehabilitatedAt = :t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":r": "revoked",
            ":a": "active",
            ":t": now
        }
    )

    return _ok("rehabilitated", thing, now)


# ---------- Helpers ----------

def _parse_body(event):
    body = event.get("body")
    if body is None:
        raise ValueError("Missing body")

    if isinstance(body, str):
        return json.loads(body)

    return body


def _validate_thing_name(body):
    thing = body.get("thingName")
    if not thing:
        raise ValueError("Missing thingName")

    if (
        not isinstance(thing, str)
        or len(thing) > 64
        or not thing.replace("_", "").replace("-", "").isalnum()
    ):
        raise ValueError("Invalid thingName")

    return thing


def _ok(action, thing, ts):
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "action": action,
            "thingName": thing,
            "timestamp": ts
        })
    }


def _bad(msg):
    return {
        "statusCode": 400,
        "body": json.dumps({"error": msg})
    }
