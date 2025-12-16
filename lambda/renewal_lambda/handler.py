import json
import os
import time
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from datetime import datetime, timezone

# ---------- Init ----------

dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ.get("DEVICE_METADATA_TABLE")
if not TABLE_NAME:
    raise RuntimeError("DEVICE_METADATA_TABLE environment variable is not set")

table = dynamodb.Table(TABLE_NAME)

# ---------- Entry point ----------
RENEWAL_PERIOD_DAYS = int(os.environ.get("RENEWAL_PERIOD_DAYS", 30)) # Lee días
RENEWAL_PERIOD_SECONDS = RENEWAL_PERIOD_DAYS * 24 * 3600



def lambda_handler(event, context):
    try:
        scope, action = _parse_path(event)
        body = _parse_body(event)

        if action == "status":
            if scope != "user":
                return _bad("Status only supported for user scope")
            return _status_user(body)

        now = int(time.time())

        targets = _resolve_targets(scope, body)
        if not targets:
            return _bad("No devices found")

        result = {"ok": [], "skipped": []}

        for user_id, thing_name in targets:
            try:
                _apply_action(user_id, thing_name, action, now)
                result["ok"].append({"userId": user_id, "thingName": thing_name})
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    result["skipped"].append({"userId": user_id, "thingName": thing_name})
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
        }))
        return _bad(str(e))

# ---------- Core logic ----------

def _apply_action(user_id, thing_name, action, now):
    key = {
        "thingName": thing_name,
    }

    if action == "renew":
        item = table.get_item(Key={"thingName": thing_name}).get("Item")
        base = max(now, int(item.get("expiresAt", 0)))
        new_expires_at = base + RENEWAL_PERIOD_SECONDS
        table.update_item(
            Key=key,
            ConditionExpression="#s = :a",
            UpdateExpression="SET lastRenewalDate = :t, expiresAt = :e",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":a": "active",
                ":t": now,
                ":e": new_expires_at,
            },
        )

    elif action == "revoke":
        table.update_item(
            Key=key,
            ConditionExpression="#s <> :r",
            UpdateExpression="SET #s = :r, revokedAt = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":r": "revoked",
                ":t": now,
            },
        )

    elif action == "rehabilitate":
        new_expires_at = now + RENEWAL_PERIOD_SECONDS
        table.update_item(
            Key=key,
            ConditionExpression="#s = :r",
            UpdateExpression=(
                "SET #s = :a, "
                "lastRenewalDate = :t, "
                "rehabilitatedAt = :t, "
                "expiresAt = :e"
            ),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":r": "revoked",
                ":a": "active",
                ":t": now,
                ":e": new_expires_at,
            },
        )

    else:
        raise ValueError("Invalid action")

# ---------- Target resolution ----------

def _resolve_targets(scope, body):
    if scope == "thing":
        thing = body.get("thingName")
        _validate_thing_name(thing)
        return [(None, thing)]

    if scope == "user":
        user_id = body.get("userId")
        if not user_id:
            raise ValueError("Missing userId")

        resp = table.query(
            IndexName="ByUser",
            KeyConditionExpression=Key("userId").eq(user_id)
        )

        return [
            (item["userId"], item["thingName"])
            for item in resp.get("Items", [])
        ]

    raise ValueError("Invalid scope")

# ---------- Status ----------

def _status_user(body):
    user_id = body.get("userId")
    if not user_id:
        raise ValueError("Missing userId")

    resp = table.query(
        IndexName="ByUser",
        KeyConditionExpression=Key("userId").eq(user_id)
    )

    devices = []
    for item in resp.get("Items", []):
        devices.append({
            "thingName": item["thingName"],
            "status": item.get("status"),
            "createdAt": _fmt_date(item.get("createdAt")),
            "lastRenewalDate": _fmt_date(item.get("lastRenewalDate")),
            "revokedAt": _fmt_date(item.get("revokedAt")),
            "rehabilitatedAt": _fmt_date(item.get("rehabilitatedAt")),
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

def _fmt_date(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        value = int(value)
    if isinstance(value, str):
        value = int(value)
    return datetime.fromtimestamp(value, tz=timezone.utc).strftime("%d/%m/%Y")

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
