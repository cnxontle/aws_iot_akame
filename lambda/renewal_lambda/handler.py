import json
import os
import time
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


# ---------- Init ----------

dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ.get("DEVICE_METADATA_TABLE")
if not TABLE_NAME:
    raise RuntimeError("DEVICE_METADATA_TABLE environment variable is not set")

table = dynamodb.Table(TABLE_NAME)


# ---------- Entry point ----------

def lambda_handler(event, context):
    try:
        scope, action = _parse_path(event)
        body = _parse_body(event)

        if action == "status":
            if scope != "user":
                return _bad("Status only supported for user scope")
            return _status_user(body)


        now = int(time.time())

        # Resolver targets
        things = _resolve_targets(scope, body)

        if not things:
            return _bad("No things found for operation")

        result = {
            "ok": [],
            "skipped": [],
        }

        for thing in things:
            try:
                _apply_action(thing, action, now)
                result["ok"].append(thing)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    result["skipped"].append(thing)
                else:
                    raise

        return {
            "statusCode": 200,
            "body": json.dumps({
                "ok": True,
                "scope": scope,
                "action": action,
                "result": result,
                "timestamp": now,
            }),
        }

    except Exception as e:
        print(json.dumps({
            "error": str(e),
            "event": event,
            "source": "device_admin_lambda",
        }))
        return _bad(str(e))


# ---------- Core logic ----------

def _apply_action(thing, action, now):
    if action == "renew":
        table.update_item(
            Key={"thingName": thing},
            ConditionExpression="attribute_exists(thingName) AND #s = :a",
            UpdateExpression="SET lastRenewalDate = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":a": "active",
                ":t": now,
            },
        )

    elif action == "revoke":
        table.update_item(
            Key={"thingName": thing},
            ConditionExpression="attribute_exists(thingName)",
            UpdateExpression="SET #s = :r, revokedAt = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":r": "revoked",
                ":t": now,
            },
        )

    elif action == "rehabilitate":
        table.update_item(
            Key={"thingName": thing},
            ConditionExpression="#s = :r",
            UpdateExpression=(
                "SET #s = :a, "
                "lastRenewalDate = :t, "
                "rehabilitatedAt = :t"
            ),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":r": "revoked",
                ":a": "active",
                ":t": now,
            },
        )

    else:
        raise ValueError("Invalid action")


# ---------- Target resolution ----------

def _resolve_targets(scope, body):
    if scope == "thing":
        thing = body.get("thingName")
        _validate_thing_name(thing)
        return [thing]

    if scope == "user":
        user_id = body.get("userId")
        if not user_id:
            raise ValueError("Missing userId")

        resp = table.query(
            IndexName="ByUser",
            KeyConditionExpression=Key("userId").eq(user_id),
        )
        return [item["thingName"] for item in resp.get("Items", [])]

    raise ValueError("Invalid scope")

# ---------- Status action ----------
def _status_user(body):
    user_id = body.get("userId")
    if not user_id:
        raise ValueError("Missing userId")

    resp = table.query(
        IndexName="ByUser",
        KeyConditionExpression=Key("userId").eq(user_id),
    )

    devices = []
    for item in resp.get("Items", []):
        devices.append({
            "thingName": item["thingName"],
            "status": item.get("status"),
            "createdAt": item.get("createdAt"),
            "lastRenewalDate": item.get("lastRenewalDate"),
            "revokedAt": item.get("revokedAt"),
            "rehabilitatedAt": item.get("rehabilitatedAt"),
        })

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "userId": user_id,
            "count": len(devices),
            "devices": devices,
        }),
    }
# ---------- Helpers ----------

def _parse_path(event):
    path = event.get("path", "")
    parts = path.strip("/").split("/")

    if len(parts) != 2:
        raise ValueError("Invalid path")

    scope, action = parts

    if scope not in ("thing", "user"):
        raise ValueError("Invalid scope")

    if action not in ("renew", "revoke", "rehabilitate", "status"):
        raise ValueError("Invalid action")

    return scope, action


def _parse_body(event):
    body = event.get("body")
    if body is None:
        raise ValueError("Missing body")

    if isinstance(body, str):
        return json.loads(body)

    return body


def _validate_thing_name(thing):
    if (
        not thing
        or not isinstance(thing, str)
        or len(thing) > 64
        or not thing.replace("_", "").replace("-", "").isalnum()
    ):
        raise ValueError("Invalid thingName")


def _bad(msg):
    return {
        "statusCode": 400,
        "body": json.dumps({"error": msg}),
    }