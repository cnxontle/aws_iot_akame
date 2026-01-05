import os
import json
import time
import re
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
athena = boto3.client("athena")

METADATA_TABLE = os.environ["METADATA_TABLE"]
ATHENA_DATABASE = os.environ["ATHENA_DATABASE"]
ATHENA_OUTPUT = os.environ["ATHENA_OUTPUT"]


table = dynamodb.Table(METADATA_TABLE)


def main(event, context):
    claims = event["requestContext"]["authorizer"]["jwt"]["claims"]
    params = event.get("queryStringParameters") or {}
    user_id = claims["sub"]
    metric = params.get("metric")
    from_ts = int(params["fromTs"]) if "fromTs" in params else None
    to_ts = int(params["toTs"]) if "toTs" in params else None

    response = table.query(
        IndexName="ByUser",
        KeyConditionExpression=Key("userId").eq(user_id),
        ProjectionExpression="thingName",
    )

    thing_names = [i["thingName"] for i in response.get("Items", [])]

    if not thing_names:
        return {"count": 0, "items": []}

    thing_list = ",".join(f"'{t}'" for t in thing_names)
    where = [f"thingName IN ({thing_list})"]

    # metric_key ONLY exists in telemetry_flattened view
    if metric:
        if not re.fullmatch(r"[a-zA-Z0-9_]+", metric):
            raise ValueError("Invalid metric format")
        where.append(f"metric_key = '{metric}'")
    
    if from_ts:
        where.append(f"timestamp >= {int(from_ts)}")

    if to_ts:
        where.append(f"timestamp <= {int(to_ts)}")


    # OBLIGATORIO: partition pruning
    if from_ts and to_ts:
        from datetime import datetime, timezone
        f = datetime.fromtimestamp(from_ts, tz=timezone.utc)
        t = datetime.fromtimestamp(to_ts, tz=timezone.utc)

        where.append(
            f"""
            (year, month, day, hour) BETWEEN
            ('{f.year}', '{f.month:02d}', '{f.day:02d}', '{f.hour:02d}')
            AND
            ('{t.year}', '{t.month:02d}', '{t.day:02d}', '{t.hour:02d}')
            """
        )

        sql = f"""
        SELECT
            thingName,
            nodeId,
            metric_key,
            metric_value,
            timestamp
        FROM telemetry.telemetry_flattened
        WHERE {" AND ".join(where)}
        ORDER BY timestamp DESC
        LIMIT 1000
        """
        qid = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": ATHENA_DATABASE},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )["QueryExecutionId"]

        start = time.time()

        while True:
            status = athena.get_query_execution(QueryExecutionId=qid)
            state = status["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED"):
                break
            if time.time() - start > 25:
                raise TimeoutError("Athena query timeout")
            time.sleep(0.5)

        if state != "SUCCEEDED":
            raise RuntimeError("Athena query failed")

        rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]

        headers = [c["VarCharValue"] for c in rows[0]["Data"]]
        items = [
            dict(zip(headers, [c.get("VarCharValue") for c in r["Data"]]))
            for r in rows[1:]
        ]

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "count": len(items),
                "items": items,
            }),
        }
    else:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
            "error": "fromTs and toTs are required"
            }),
        }

