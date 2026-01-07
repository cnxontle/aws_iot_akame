import os
import json
import time
import re
import boto3
from datetime import datetime, timezone, timedelta
from boto3.dynamodb.conditions import Key


MAX_RANGE_SECONDS = 24 * 60 * 60
MAX_ROWS = 1000

dynamodb = boto3.resource("dynamodb")
athena = boto3.client("athena")

TABLE = dynamodb.Table(os.environ["METADATA_TABLE"])
DB = os.environ["ATHENA_DATABASE"]
OUTPUT = os.environ["ATHENA_OUTPUT"]
WORKGROUP = os.environ["ATHENA_WORKGROUP"]


def error(status, message):
    print("ERROR:", message)  # LOG
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}),
    }


def validate_query_params(params):
    try:
        from_ts = int(params["fromTs"])
        to_ts = int(params["toTs"])
    except Exception as e:
        return error(400, f"Invalid timestamps: {str(e)}")

    if to_ts <= from_ts:
        return error(400, "toTs must be greater than fromTs")

    if to_ts - from_ts > MAX_RANGE_SECONDS:
        return error(422, "Time range exceeds 24h limit")

    metric = params.get("metric")
    if metric and not re.fullmatch(r"[a-zA-Z0-9_]+", metric):
        return error(400, "Invalid metric format")

    return {"from_ts": from_ts, "to_ts": to_ts, "metric": metric}


def main(event, context):
    print("=== EVENT ===")
    print(json.dumps(event))

    try:
        claims = event["requestContext"]["authorizer"]["jwt"]["claims"]
        user_id = claims["sub"]
    except Exception as e:
        return error(401, f"Invalid JWT claims: {str(e)}")

    params = event.get("queryStringParameters") or {}
    validation = validate_query_params(params)
    if not isinstance(validation, dict):
        return validation

    from_ts = validation["from_ts"]
    to_ts = validation["to_ts"]
    metric = validation["metric"]

    # ------ DYNAMO LOOKUP ------
    thing_names = []
    try:
        resp = TABLE.query(
            IndexName="ByUser",
            KeyConditionExpression=Key("userId").eq(user_id),
            ProjectionExpression="thingName",
        )
        thing_names.extend([i["thingName"] for i in resp.get("Items", [])])

        while "LastEvaluatedKey" in resp:
            resp = TABLE.query(
                IndexName="ByUser",
                KeyConditionExpression=Key("userId").eq(user_id),
                ProjectionExpression="thingName",
                ExclusiveStartKey=resp["LastEvaluatedKey"],
            )
            thing_names.extend([i["thingName"] for i in resp.get("Items", [])]) 
    except Exception as e:
        return error(500, f"DynamoDB query failed: {str(e)}")

    if not thing_names:
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"count": 0, "items": []}),
        }

    # Construct WHERE clauses
    quoted = ", ".join([f"'{t}'" for t in thing_names])
    where = [f"thingName IN ({quoted})"]


    if metric:
        where.append(f"{metric} IS NOT NULL")

    where.append(
        f"timestamp >= {from_ts} AND timestamp <= {to_ts}"
    )

    f = datetime.fromtimestamp(from_ts, tz=timezone.utc)
    t = datetime.fromtimestamp(to_ts, tz=timezone.utc)

    partition_filters = []

    current = f.replace(minute=0, second=0, microsecond=0)

    while current <= t:
        partition_filters.append(
            f"(year='{current.year}' AND month='{current.month:02d}' AND day='{current.day:02d}' AND hour='{current.hour:02d}')"
        )
        current += timedelta(hours=1)

    # Añadir particiones específicas
    where.append("(" + " OR ".join(partition_filters) + ")")

    # FULL SQL
    sql = f"""
        SELECT *
        FROM telemetry.telemetry_flattened
        WHERE {" AND ".join(where)}
        ORDER BY timestamp DESC
        LIMIT {MAX_ROWS}
    """

    print("=== ATHENA QUERY ===")
    print(sql)
    print("DB:", DB)
    print("WorkGroup:", WORKGROUP)
    print("Output:", OUTPUT)

    # ------ START ATHENA QUERY ------
    try:
        qid = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DB},
            WorkGroup=WORKGROUP,
            ResultConfiguration={"OutputLocation": OUTPUT},
            ResultReuseConfiguration={
                "ResultReuseByAgeConfiguration": {
                    "Enabled": True,
                    "MaxAgeInMinutes": 10
                }
            }
        )["QueryExecutionId"]

        print("QueryExecutionId:", qid)

    except Exception as e:
        return error(500, f"Athena start_query_execution failed: {str(e)}")

    # ------ WAIT FOR COMPLETION ------
    try:
        start = time.time()
        sleep_time = 0.5

        while True:
            status = athena.get_query_execution(QueryExecutionId=qid)
            state = status["QueryExecution"]["Status"]["State"]

            if state in ("SUCCEEDED", "FAILED"):
                break

            if time.time() - start > 30:
                return error(504, "Athena query timeout")

            time.sleep(sleep_time)
            sleep_time = min(sleep_time * 1.5, 4)


        if state != "SUCCEEDED":
            print("Athena failure:", status)
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            return error(500, f"Athena query failed: {reason}")
 
    except Exception as e:
        return error(500, f"Athena get_query_execution failed: {str(e)}")

    # ------ FETCH RESULTS ------
    try:
        result = athena.get_query_results(QueryExecutionId=qid)
        rows = result["ResultSet"]["Rows"]
        headers = [c["VarCharValue"] for c in rows[0]["Data"]]

        items = [
            dict(zip(headers, [c.get("VarCharValue") for c in r["Data"]]))
            for r in rows[1:]
        ]

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"count": len(items), "items": items}),
        }

    except Exception as e:
        return error(500, f"Athena get_query_results failed: {str(e)}")
