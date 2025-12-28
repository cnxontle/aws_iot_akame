import os
import time
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")

ACTIVATION_CODE_TABLE = os.environ["ACTIVATION_CODE_TABLE"]
table = dynamodb.Table(ACTIVATION_CODE_TABLE)


def main(event, context):
    """
    Consume un activation code y lo vincula a un usuario Cognito
    """
    try:
        activation_code = event.get("activationCode")
        cognito_sub = event.get("cognitoSub")

        if not activation_code or not isinstance(activation_code, str):
            return _error("invalid activationCode")

        if not cognito_sub or not isinstance(cognito_sub, str):
            return _error("invalid cognitoSub")

        now = int(time.time())

        try:
            response = table.update_item(
                Key={"code": activation_code},
                UpdateExpression="""
                    SET #s = :used,
                        usedAt = :now,
                        cognitoSub = :cognitoSub
                """,
                ConditionExpression="""
                    #s = :active AND expiresAt > :now
                """,
                ExpressionAttributeNames={
                    "#s": "status",
                },
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

        return {
            "status": "ok",
            "userId": item["userId"],
            "activatedAt": now,
        }

    except Exception as e:
        print("ConsumeActivationCode error:", str(e))
        return _error("internal error")


def _error(message):
    return {
        "status": "error",
        "message": message
    }
