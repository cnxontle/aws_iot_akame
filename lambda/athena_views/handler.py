import os
import time
import boto3

athena = boto3.client("athena")

DATABASE = os.environ["ATHENA_DATABASE"]
OUTPUT = os.environ["ATHENA_OUTPUT"]

VIEW_SQL = """
CREATE OR REPLACE VIEW telemetry.telemetry_flattened AS
SELECT
    r.meshid,
    r.event_ts AS timestamp,
    r.ingestedAt,
    rd.nodeId,

    rd.humidity,
    rd.raw,

    rd.soil_moisture,
    rd.soil_temperature,
    rd.soil_ph,
    rd.soil_ec,
    rd.soil_nitrogen,
    rd.soil_phosphorus,
    rd.soil_potassium,
    rd.soil_salinity,

    rd.air_temperature,
    rd.air_humidity,
    rd.air_pressure,
    rd.wind_speed,
    rd.rainfall,
    rd.solar_radiation,
    rd.co2_level,
    rd.leaf_wetness,

    rd.pm1,
    rd.pm2_5,
    rd.pm10,
    rd.voc,
    rd.o3_level,
    rd.no2_level,
    rd.so2_level,

    rd.battery_voltage,
    rd.battery_level,
    rd.battery_health,
    rd.signal_strength,
    rd.device_temperature,
    rd.uptime,

    r.year
FROM telemetry.telemetry_raw r
CROSS JOIN UNNEST(r.readings) AS t(rd)
WHERE r.meshid IS NOT NULL;

"""


def main(event, context):
    if event["RequestType"] == "Delete":
        return {"status": "skipped"}

    res = athena.start_query_execution(
        QueryString=VIEW_SQL,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=os.environ["ATHENA_WORKGROUP"],
        ResultConfiguration={"OutputLocation": OUTPUT},
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
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "No reason provided")
        raise RuntimeError(f"Athena query FAILED: {state} - {reason}")

    return {"status": "ok"}
