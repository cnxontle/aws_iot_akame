import os
import time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")

# Tabla de códigos de activación
ACTIVATION_CODE_TABLE = os.environ["ACTIVATION_CODE_TABLE"]
activation_table = dynamodb.Table(ACTIVATION_CODE_TABLE)

# Tabla de metadata de dispositivos
DEVICE_METADATA_TABLE = os.environ["DEVICE_METADATA_TABLE"]
device_table = dynamodb.Table(DEVICE_METADATA_TABLE)

def main(event, context):
    """
    Consume un activation code y lo vincula a un usuario Cognito (cognitoSub),
    actualizando también la tabla DeviceMetadata para asignar userId al Thing.
    """
    try:
        activation_code = event.get("activationCode")
        cognito_sub = event.get("cognitoSub")  # userId del usuario final en Cognito

        if not activation_code or not isinstance(activation_code, str):
            return _error("invalid activationCode")

        if not cognito_sub or not isinstance(cognito_sub, str):
            return _error("invalid cognitoSub")

        now = int(time.time())

        # ------------------------
        # Actualizar ActivationCodeTable
        # ------------------------
        try:
            response = activation_table.update_item(
                Key={"code": activation_code},
                UpdateExpression="""
                    SET #s = :used,
                        usedAt = :now,
                        cognitoSub = :cognitoSub
                """,
                ConditionExpression="""
                    #s = :active AND expiresAt > :now
                """,
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":active": "active",
                    ":used": "used",
                    ":now": now,
                    ":cognitoSub": cognito_sub,
                },
                ReturnValues="ALL_NEW",
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _error("activation code invalid, expired or already used")
            raise

        item = response["Attributes"]
        user_id_admin = item["userId"]  # ID original del admin que generó el código

        # ------------------------
        # Actualizar DeviceMetadata para asignar userId
        # ------------------------
        # Buscamos el Thing asociado a este código de activación
        resp = device_table.query(
            IndexName="ByActivationCode",
            KeyConditionExpression=Key("activationCode").eq(activation_code),
            Limit=1
        )

        if resp["Count"] == 1:
            thing_name = resp["Items"][0]["thingName"]
            # Actualizamos userId del Thing con cognitoSub
            device_table.update_item(
                Key={"thingName": thing_name},
                UpdateExpression="SET userId = :uid",
                ExpressionAttributeValues={":uid": cognito_sub}
            )
        else:
            print(f"No Thing found for activation code {activation_code}")

        # ------------------------
        # Retorno
        # ------------------------
        return {
            "status": "ok",
            "userId": user_id_admin,  # ID asignado por admin
            "activatedAt": now,
        }

    except Exception as e:
        print("ConsumeActivationCode error:", str(e))
        return _error("internal error")

def _error(message):
    return {"status": "error", "message": message}
