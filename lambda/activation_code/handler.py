import os
import time
import secrets
import string
import boto3

TABLE_NAME = os.environ["ACTIVATION_CODE_TABLE"]
DEFAULT_TTL = int(os.environ.get("DEFAULT_CODE_TTL_SECONDS", 604800))
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


def _generate_code(length=10):
    alphabet = string.ascii_uppercase + string.digits
    return "ACT-" + "".join(secrets.choice(alphabet) for _ in range(length))


def main(event, context):
    user_id = event.get("userId")
    ttl_seconds = event.get("ttlSeconds", DEFAULT_TTL)

    if not user_id or not isinstance(user_id, str):
        return _error("invalid userId")

    if ttl_seconds <= 0 or ttl_seconds > 30 * 24 * 3600:
        return _error("invalid ttlSeconds")

    now = int(time.time())
    expires_at = now + ttl_seconds

    # Generar código único (retry simple)
    for _ in range(3):
        code = _generate_code()
        try:
            table.put_item(
                Item={
                    "code": code,
                    "userId": user_id,
                    "status": "active",
                    "createdAt": now,
                    "expiresAt": expires_at,
                    "usedAt": None
                },
                ConditionExpression="attribute_not_exists(code)",
            )
            break
        except Exception:
            code = None

    if not code:
        return _error("could not generate unique code")

    return {
        "status": "ok",
        "activationCode": code,
        "userId": user_id,
        "expiresAt": expires_at,
    }


def _error(msg):
    return {"status": "error", "message": msg}
