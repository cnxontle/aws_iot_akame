import json
import boto3
import base64
import os

dynamodb = boto3.client("dynamodb")
TABLE = os.environ["DYNAMO_TABLE"]


def handler(event, context):
    output = []

    for record in event["records"]:
        raw = base64.b64decode(record["data"]).decode("utf-8")

        try:
            data = json.loads(raw)
        except:
            data = {"raw": raw}

        mesh_id = data.get("meshId")

        # Si no viene meshId → enviar unknown
        if not mesh_id:
            partition_key = "unknown"
        else:
            # Validar en DynamoDB
            try:
                response = dynamodb.get_item(
                    TableName=TABLE,
                    Key={"thingName": {"S": mesh_id}},
                )
            except Exception as e:
                print(f"Error DynamoDB for meshId {mesh_id}: {str(e)}")
                partition_key = "dynamodb_error"

            if "Item" not in response:
                partition_key = "unknown_" + mesh_id
            else:
                partition_key = mesh_id  # SOLO meshId sin userId

        # No afecta el payload, solo metadata
        new_payload = json.dumps(data) + "\n"

        output.append({
            "recordId": record["recordId"],
            "result": "Ok",
            "data": base64.b64encode(new_payload.encode("utf-8")).decode("utf-8"),
            "metadata": {
                "partitionKeys": {
                    "meshId": partition_key
                }
            }
        })

    return {"records": output}
