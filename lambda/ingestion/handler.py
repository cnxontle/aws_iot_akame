import base64
import json

def handler(event, context):
    output = []

    for record in event["records"]:
        raw_data = record["data"]

        try:
            # Decode base64
            payload = base64.b64decode(raw_data)
            data = json.loads(payload)

            # meshId viene del IoT Rule
            mesh_id = data.get("meshId", "unknown")

            transformed_record = {
                "recordId": record["recordId"],
                "result": "Ok",
                # retornamos EXACTAMENTE el payload original
                "data": raw_data,
                "metadata": {
                    "partitionKeys": {
                        "meshId": mesh_id
                    }
                }
            }

        except Exception as e:
            # Si un record falla, no detenemos el lote
            transformed_record = {
                "recordId": record["recordId"],
                "result": "ProcessingFailed",
                "data": raw_data
            }

        output.append(transformed_record)

    return {"records": output}
