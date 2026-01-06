import os
import time
import boto3

athena = boto3.client("athena")

DATABASE = os.environ["ATHENA_DATABASE"]
OUTPUT = os.environ["ATHENA_OUTPUT"]

VIEW_SQL = """
CREATE OR REPLACE VIEW telemetry.telemetry_flattened AS
SELECT
    r.meshId,
    r.thingName,
    r.event_ts AS timestamp,
    r.ingestedAt,
    rd.nodeId,
    m.key   AS metric_key,
    m.value AS metric_value,
    year,
    month,
    day,
    hour
FROM telemetry.telemetry_raw r
CROSS JOIN UNNEST(r.readings) AS t(rd)
CROSS JOIN UNNEST(rd.metrics) AS m
WHERE
   year IS NOT NULL
   AND month IS NOT NULL
   AND day IS NOT NULL
   AND hour IS NOT NULL;
"""


def main(event, context):
    if event["RequestType"] == "Delete":
        return {"status": "skipped"}

    res = athena.start_query_execution(
        QueryString=VIEW_SQL,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=os.environ["ATHENA_WORKGROUP"],
    )

    qid = res["QueryExecutionId"]

    start = time.time()

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED"):
            break
        if time.time() - start > 25:
            raise TimeoutError("Failed to create Athena view")

        time.sleep(1)

    if state != "SUCCEEDED":
        raise RuntimeError("Failed to create Athena view")

    return {"status": "ok"}
