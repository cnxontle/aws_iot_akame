import json
import time
import os
import boto3

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["DEVICE_METADATA_TABLE"]

table = dynamodb.Table(TABLE_NAME)

def handler(event, context):
    now = int(time.time())

    # Desde la IoT Rule
    thing_name = event.get("clientid")

    if not thing_name:
        print("DROP: no clientid")
        return

    resp = table.get_item(Key={"thingName": thing_name})
    item = resp.get("Item")

    if not item:
        print(f"DROP: thing not registered {thing_name}")
        return

    expires_at = int(item.get("expiresAt", 0))
    status = item.get("status", "inactive")

    # TU CONDICIÓN CLAVE
    if now > expires_at or status != "active":
        print(f"DROP: expired or inactive {thing_name}")
        return

    # Mensaje válido
    print("ACCEPT:", json.dumps(event))

   
