import os
import time
import re
import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

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


def main(event, context):
    claims = event["requestContext"]["authorizer"]["jwt"]["claims"]
    params = event.get("queryStringParameters") or {}
    user_id = claims["sub"]
    metrics = [
        m for m in params.get("metrics", "").split(",")
        if re.match(r"^[a-zA-Z0-9_]+$", m)
    ]
    if not metrics:
        raise ValueError("At least one metric is required")
    interval = params.get("interval", "day")
    from_ts = int(params["fromTs"])
    to_ts = int(params["toTs"])
    
    if interval not in INTERVAL_SQL:
        raise ValueError("Invalid interval")


    # Resolver devices del usuario
    resp = TABLE.query(
        IndexName="ByUser",
        KeyConditionExpression=Key("userId").eq(user_id),
        ProjectionExpression="thingName",
    )

    things = [i["thingName"] for i in resp.get("Items", [])]
    if not things:
        return {"interval": interval, "series": {}}

    thing_list = ",".join(f"'{t}'" for t in things)
    metric_list = ",".join(f"'{m}'" for m in metrics)

    # Partition pruning (CRÍTICO para costos)
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
      AND (
        year BETWEEN '{f.year}' AND '{t.year}'
        AND month BETWEEN '{f.month:02d}' AND '{t.month:02d}'
        AND day BETWEEN '{f.day:02d}' AND '{t.day:02d}'
        AND hour BETWEEN '{f.hour:02d}' AND '{t.hour:02d}'
      )
    GROUP BY metric_key, bucket
    ORDER BY bucket
    """

    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": OUTPUT},
    )["QueryExecutionId"]

    start = time.time()

    while True:
        s = athena.get_query_execution(QueryExecutionId=qid)
        state = s["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED"):
            break
        if time.time() - start > 25:
            raise TimeoutError("Athena query timeout")
        time.sleep(0.5)

    if state != "SUCCEEDED":
        raise RuntimeError("Athena query failed")

    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]

    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    data = rows[1:]

    series = {}
    for row in data:
        r = dict(zip(headers, [c.get("VarCharValue") for c in row["Data"]]))
        m = r.pop("metric_key")
        series.setdefault(m, []).append(r)

    return {
        "interval": interval,
        "series": series,
    }
