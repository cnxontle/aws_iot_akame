import os
import time
import secrets
import string
import boto3

TABLE_NAME = os.environ["ACTIVATION_CODE_TABLE"]
DEFAULT_TTL = int(os.environ.get("DEFAULT_CODE_TTL_SECONDS", 604800))  # 7 días
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


# Genera un código único
def _generate_code(length=10):
    alphabet = string.ascii_uppercase + string.digits
    return "ACT-" + "".join(secrets.choice(alphabet) for _ in range(length))


# Lambda handler
def main(event, context):
    user_id = event.get("userId")  # ID del usuario Cognito que el admin quiere asignar


    if not user_id or not isinstance(user_id, str):
        return _error("invalid userId")


    now = int(time.time())


    # Generar código único (retry simple)
    for _ in range(3):
        code = _generate_code()
        try:
            table.put_item(
                Item={
                    "code": code,
                    "userId": user_id,
                    "status": "active",  # activo hasta usar
                    "createdAt": now,
                    "usedAt": None
                },
                ConditionExpression="attribute_not_exists(code)",  # evita colisiones
            )
            break
        except Exception:
            code = None

    if not code:
        return _error("could not generate unique code")

    # Retorna código generado para que el admin se lo entregue al usuario
    return {
        "status": "ok",
        "activationCode": code,
        "userId": user_id,
    }

def _error(msg):
    return {"status": "error", "message": msg}