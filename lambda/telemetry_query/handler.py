import os
import json
import time
import re
import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

# Límites
MAX_RANGE_SECONDS = 24 * 60 * 60  # 24h
MAX_ROWS = 1000

# AWS clients
dynamodb = boto3.resource("dynamodb")
athena = boto3.client("athena")

TABLE = dynamodb.Table(os.environ["METADATA_TABLE"])
DB = os.environ["ATHENA_DATABASE"]
OUTPUT = os.environ["ATHENA_OUTPUT"]

# Helpers
def error(status, message):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}),
    }


def validate_query_params(params):
    try:
        from_ts = int(params["fromTs"])
        to_ts = int(params["toTs"])
    except (KeyError, ValueError):
        return error(400, "fromTs and toTs are required and must be integers")

    if to_ts <= from_ts:
        return error(400, "toTs must be greater than fromTs")

    if to_ts - from_ts > MAX_RANGE_SECONDS:
        return error(422, "Time range exceeds 24h limit")

    metric = params.get("metric")
    if metric and not re.fullmatch(r"[a-zA-Z0-9_]+", metric):
        return error(400, "Invalid metric format")

    return {
        "from_ts": from_ts,
        "to_ts": to_ts,
        "metric": metric,
    }

# Lambda handler
def main(event, context):
    claims = event["requestContext"]["authorizer"]["jwt"]["claims"]
    params = event.get("queryStringParameters") or {}
    user_id = claims["sub"]

    validation = validate_query_params(params)
    if not isinstance(validation, dict):
        return validation

    from_ts = validation["from_ts"]
    to_ts = validation["to_ts"]
    metric = validation["metric"]

    # ─── Resolver dispositivos del usuario ───
    response = TABLE.query(
        IndexName="ByUser",
        KeyConditionExpression=Key("userId").eq(user_id),
        ProjectionExpression="thingName",
    )

    thing_names = [i["thingName"] for i in response.get("Items", [])]
    if not thing_names:
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"count": 0, "items": []}),
        }
    where = [f"thingName IN ({', '.join(f'\'{t}\'' for t in thing_names)})"]
 

    if metric:
        where.append(f"metric_key = '{metric}'")

    where.append(f"timestamp BETWEEN {from_ts} AND {to_ts}")

    # ─── Partition pruning obligatorio ───
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
    LIMIT {MAX_ROWS}
    """

    # ─── Athena ───
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        WorkGroup=os.environ["ATHENA_WORKGROUP"],
    )["QueryExecutionId"]

    start = time.time()
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED"):
            break
        if time.time() - start > 20:
            return error(504, "Athena query timeout")
        time.sleep(0.5)

    if state != "SUCCEEDED":
        return error(500, "Athena query failed")

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
