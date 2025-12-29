import json
import os
import time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from decimal import Decimal
from datetime import datetime, timezone

# ---------- Init ----------

dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["DEVICE_METADATA_TABLE"]
table = dynamodb.Table(TABLE_NAME)

RENEWAL_PERIOD_DAYS = int(os.environ.get("RENEWAL_PERIOD_DAYS", 30))
RENEWAL_PERIOD_SECONDS = RENEWAL_PERIOD_DAYS * 86400


# ---------- Entry ----------

def lambda_handler(event, context):
    try:
        scope, action = _parse_path(event)
        body = _parse_body(event)
        source = body.get("source", "admin")

        # Seguridad: pagos solo pueden renovar usuarios
        if source == "payment":
            if scope != "user" or action != "renew":
                return _bad("Payment source only allowed for user renew")

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
                _apply_action(thing_name, action, now, source)
                result["ok"].append({"thingName": thing_name})
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    result["skipped"].append({"thingName": thing_name})
                else:
                    raise

        return {
            "statusCode": 200,
            "body": json.dumps({
                "ok": True,
                "scope": scope,
                "action": action,
                "source": source,
                "result": result,
                "timestamp": now,
            }),
        }

    except Exception as e:
        print("Renewal error:", e)
        return _bad(str(e))


# ---------- Core logic ----------

def _apply_action(thing_name, action, now, source):
    item = table.get_item(Key={"thingName": thing_name}).get("Item")
    if not item:
        raise ValueError("Thing not found")

    status = item.get("status")
    lifecycle = item.get("lifecycleStatus", "ACTIVE")

    if action == "renew":
        # Nunca renovar revocados
        if status == "revoked":
            raise ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException"}},
                "UpdateItem"
            )

        base = max(now, int(item.get("expiresAt", 0)))
        new_expires_at = base + RENEWAL_PERIOD_SECONDS

        table.update_item(
            Key={"thingName": thing_name},
            UpdateExpression="""
                SET lastRenewalDate = :t,
                    expiresAt = :e,
                    lifecycleStatus = :l,
                    renewalSource = :s
            """,
            ExpressionAttributeValues={
                ":t": now,
                ":e": new_expires_at,
                ":l": "ACTIVE",
                ":s": source,
            },
        )

    elif action == "revoke":
        table.update_item(
            Key={"thingName": thing_name},
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
            Key={"thingName": thing_name},
            ConditionExpression="#s = :r",
            UpdateExpression="""
                SET #s = :a,
                    lifecycleStatus = :l,
                    rehabilitatedAt = :t,
                    lastRenewalDate = :t,
                    expiresAt = :e
            """,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":r": "revoked",
                ":a": "active",
                ":l": "ACTIVE",
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

        return [(item["userId"], item["thingName"]) for item in resp.get("Items", [])]

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
            "lifecycleStatus": item.get("lifecycleStatus"),
            "expiresAt": _fmt_date(item.get("expiresAt")),
            "lastRenewalDate": _fmt_date(item.get("lastRenewalDate")),
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
    return json.loads(body) if isinstance(body, str) else body


def _validate_thing_name(name):
    if not name or not isinstance(name, str) or len(name) > 64:
        raise ValueError("Invalid thingName")


def _fmt_date(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        value = int(value)
    return datetime.fromtimestamp(int(value), tz=timezone.utc).strftime("%Y-%m-%d")


def _bad(msg):
    return {
        "statusCode": 400,
        "body": json.dumps({"error": msg}),
    }
