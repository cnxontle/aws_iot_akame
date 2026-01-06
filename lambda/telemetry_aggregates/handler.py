import os
import time
import re
import json
import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key


# Límites 
MAX_RANGE_SECONDS = 365 * 24 * 60 * 60  # 365 días
MAX_METRICS = 5
ALLOWED_INTERVALS = {"day", "week", "month", "year"}


# AWS clients
athena = boto3.client("athena")
dynamodb = boto3.resource("dynamodb")

TABLE = dynamodb.Table(os.environ["METADATA_TABLE"])
DB = os.environ["ATHENA_DATABASE"]
OUTPUT = os.environ["ATHENA_OUTPUT"]

INTERVAL_SQL = {
    "day":   "date(from_unixtime(timestamp))",
    "week":  "date_trunc('week', from_unixtime(timestamp))",
    "month": "date_trunc('month', from_unixtime(timestamp))",
    "year":  "date_trunc('year', from_unixtime(timestamp))",
}

# Helpers
def error(status, message):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}),
    }


def validate_aggregates_query(params):
    # ---- timestamps ----
    try:
        from_ts = int(params["fromTs"])
        to_ts = int(params["toTs"])
    except (KeyError, ValueError):
        return error(400, "fromTs and toTs are required and must be integers")

    if to_ts <= from_ts:
        return error(400, "toTs must be greater than fromTs")

    if to_ts - from_ts > MAX_RANGE_SECONDS:
        return error(422, "Time range exceeds 365 days limit")

    # ---- metrics ----
    metrics_raw = params.get("metrics")
    if not metrics_raw:
        return error(400, "metrics parameter is required")

    metrics = [
        m for m in metrics_raw.split(",")
        if re.fullmatch(r"[a-zA-Z0-9_]+", m)
    ]

    if not metrics:
        return error(400, "Invalid metrics format")

    if len(metrics) > MAX_METRICS:
        return error(422, f"Maximum {MAX_METRICS} metrics allowed")

    # ---- interval ----
    interval = params.get("interval", "day")
    if interval not in ALLOWED_INTERVALS:
        return error(
            422,
            f"Invalid interval '{interval}'. Allowed: {', '.join(ALLOWED_INTERVALS)}",
        )

    return {
        "from_ts": from_ts,
        "to_ts": to_ts,
        "metrics": metrics,
        "interval": interval,
    }

# Lambda handler
def main(event, context):
    claims = event["requestContext"]["authorizer"]["jwt"]["claims"]
    params = event.get("queryStringParameters") or {}
    user_id = claims["sub"]

    # ─── VALIDACIÓN (gatekeeper de Athena) ───
    validation = validate_aggregates_query(params)
    if not isinstance(validation, dict):
        return validation

    from_ts = validation["from_ts"]
    to_ts = validation["to_ts"]
    metrics = validation["metrics"]
    interval = validation["interval"]

    # Resolver dispositivos del usuario
    resp = TABLE.query(
        IndexName="ByUser",
        KeyConditionExpression=Key("userId").eq(user_id),
        ProjectionExpression="thingName",
    )

    things = [i["thingName"] for i in resp.get("Items", [])]
    if not things:
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"interval": interval, "series": {}}),
        }

    thing_list = ",".join(f"'{t}'" for t in things)
    metric_list = ",".join(f"'{m}'" for m in metrics)

    # Partition pruning
    f = datetime.fromtimestamp(from_ts, tz=timezone.utc)
    t = datetime.fromtimestamp(to_ts, tz=timezone.utc)

    bucket_expr = INTERVAL_SQL[interval]

    sql = f"""
    SELECT
      metric_key,
      {bucket_expr} AS bucket,
      avg(metric_value) AS avg,
      min(metric_value) AS min,
      max(metric_value) AS max,
      count(*) AS count
    FROM telemetry.telemetry_flattened
    WHERE
      thingName IN ({thing_list})
      AND metric_key IN ({metric_list})
      AND timestamp BETWEEN {from_ts} AND {to_ts}
      AND (year, month, day, hour) BETWEEN
        ('{f.year}', '{f.month:02d}', '{f.day:02d}', '{f.hour:02d}')
        AND
        ('{t.year}', '{t.month:02d}', '{t.day:02d}', '{t.hour:02d}')
    GROUP BY metric_key, bucket
    ORDER BY bucket
    """

    # Ejecutar Athena
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
        if time.time() - start > 25:
            return error(504, "Athena query timeout")
        time.sleep(0.5)

    if state != "SUCCEEDED":
        return error(500, "Athena query failed")

    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    headers = [c["VarCharValue"] for c in rows[0]["Data"]]

    series = {}
    for row in rows[1:]:
        r = dict(zip(headers, [c.get("VarCharValue") for c in row["Data"]]))
        metric = r.pop("metric_key")
        series.setdefault(metric, []).append(r)

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "interval": interval,
            "series": series,
        }),
    }
