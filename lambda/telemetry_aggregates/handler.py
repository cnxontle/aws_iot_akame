import json
import os
import time
import boto3
from datetime import datetime, timezone

athena = boto3.client("athena")

DATABASE = os.environ["ATHENA_DATABASE"]
WORKGROUP = os.environ["ATHENA_WORKGROUP"]

# límites defensivos
MAX_RANGE_DAYS = 365
MAX_METRICS = 5
ALLOWED_INTERVALS = {"day", "week", "month", "year"}

# métricas permitidas (deben existir como columnas)
ALLOWED_METRICS = {
    "humidity", "raw", "soil_moisture", "soil_temperature", "soil_ph", "soil_ec",
    "soil_nitrogen", "soil_phosphorus", "soil_potassium", "soil_salinity",
    "air_temperature", "air_humidity", "air_pressure", "wind_speed", "rainfall",
    "solar_radiation", "co2_level", "leaf_wetness",
    "pm1", "pm2_5", "pm10", "voc", "o3_level", "no2_level", "so2_level",
    "battery_voltage", "battery_level", "battery_health", "signal_strength",
    "device_temperature"
}

TABLE = "telemetry.telemetry_flattened"


def _bucket_expr(interval: str) -> str:
    if interval == "day":
        return "date_trunc('day', timestamp)"
    if interval == "week":
        return "date_trunc('week', timestamp)"
    if interval == "month":
        return "date_trunc('month', timestamp)"
    if interval == "year":
        return "date_trunc('year', timestamp)"
    raise ValueError("Invalid interval")


def handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))

        things = body.get("things", [])
        metrics = body.get("metrics", [])
        interval = body.get("interval")
        from_ts = body.get("from")
        to_ts = body.get("to")


        # Validaciones
        if not things:
            return _err(400, "things is required")

        if not metrics or len(metrics) > MAX_METRICS:
            return _err(400, f"metrics must be 1..{MAX_METRICS}")

        if any(m not in ALLOWED_METRICS for m in metrics):
            return _err(400, "one or more metrics not allowed")

        if interval not in ALLOWED_INTERVALS:
            return _err(400, "invalid interval")

        if not from_ts or not to_ts:
            return _err(400, "from and to are required")

        f = datetime.fromtimestamp(from_ts, tz=timezone.utc)
        t = datetime.fromtimestamp(to_ts, tz=timezone.utc)

        if (t - f).days > MAX_RANGE_DAYS:
            return _err(400, f"max range is {MAX_RANGE_DAYS} days")


        # SQL building
        bucket = _bucket_expr(interval)

        thing_list = ", ".join(f"'{t}'" for t in things)

        partition_filter = f"""
        (year, month, day, hour) BETWEEN
            ('{f.year}', '{f.month:02d}', '{f.day:02d}', '{f.hour:02d}')
        AND
            ('{t.year}', '{t.month:02d}', '{t.day:02d}', '{t.hour:02d}')
        """

        queries = []

        for metric in metrics:
            queries.append(f"""
                SELECT
                    '{metric}' AS metric,
                    {bucket} AS bucket,
                    avg({metric}) AS avg,
                    min({metric}) AS min,
                    max({metric}) AS max,
                    count({metric}) AS count
                FROM {TABLE}
                WHERE
                    thingname IN ({thing_list})
                    AND {metric} IS NOT NULL
                    AND timestamp BETWEEN
                        from_unixtime({from_ts})
                        AND from_unixtime({to_ts})
                    AND {partition_filter}
                GROUP BY bucket
            """)

        sql = " UNION ALL ".join(queries) + " ORDER BY bucket"


        # Athena execution
        res = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE},
            WorkGroup=WORKGROUP,
        )

        qid = res["QueryExecutionId"]

        _wait_for_query(qid)

        rows = _fetch_results(qid)


        # Response shaping
        series = {}
        for r in rows:
            metric = r["metric"]
            series.setdefault(metric, []).append({
                "bucket": r["bucket"],
                "avg": r["avg"],
                "min": r["min"],
                "max": r["max"],
                "count": r["count"],
            })

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "interval": interval,
                "from": from_ts,
                "to": to_ts,
                "series": series
            }),
        }

    except Exception as e:
        print("ERROR:", str(e))
        return _err(500, "internal error")



# Helpers
def _wait_for_query(qid: str):
    while True:
        res = athena.get_query_execution(QueryExecutionId=qid)
        state = res["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(
                res["QueryExecution"]["Status"].get("StateChangeReason")
            )

        time.sleep(0.5)


def _fetch_results(qid: str):
    paginator = athena.get_paginator("get_query_results")
    rows = []

    for page in paginator.paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"][1:]:
            d = [c.get("VarCharValue") for c in r["Data"]]
            rows.append({
                "metric": d[0],
                "bucket": d[1],
                "avg": float(d[2]) if d[2] else None,
                "min": float(d[3]) if d[3] else None,
                "max": float(d[4]) if d[4] else None,
                "count": int(d[5]) if d[5] else 0,
            })

    return rows


def _err(code, msg):
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": msg}),
    }
