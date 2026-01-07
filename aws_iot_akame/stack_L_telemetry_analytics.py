from aws_cdk import (
    Stack,
    Duration,
    aws_glue as glue,
    aws_s3 as s3,
)
from constructs import Construct

class TelemetryAnalyticsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        telemetry_bucket_name: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        athena_output_bucket = s3.Bucket(
            self,
            "AthenaOutputBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(expiration=Duration.days(30))
            ],
        )

        # 1. Glue Database
        database = glue.CfnDatabase(
            self,
            "TelemetryDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="telemetry"
            ),
        )

        # 2. RAW table (schema flexible)
        glue.CfnTable(
            self,
            "TelemetryRawTable",
            catalog_id=self.account,
            database_name=database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name="telemetry_raw",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "json",
                    "compressionType": "gzip",
                    "json.open.content": "true",

                    # PARTITION PROJECTION
                    "projection.enabled": "true",

                    "projection.year.type": "integer",
                    "projection.year.range": "2023,2100",

                    "projection.month.type": "integer",
                    "projection.month.range": "1,12",
                    "projection.month.digits": "2",

                    "projection.day.type": "integer",
                    "projection.day.range": "1,31",
                    "projection.day.digits": "2",

                    "projection.hour.type": "integer",
                    "projection.hour.range": "0,23",
                    "projection.hour.digits": "2",


                    # cómo construir el path
                    "storage.location.template": (
                        f"s3://{telemetry_bucket_name}/"
                        "year=${year}/"
                        "month=${month}/"
                        "day=${day}/"
                        "hour=${hour}/"
                    ),
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="year", type="string"),
                    glue.CfnTable.ColumnProperty(name="month", type="string"),
                    glue.CfnTable.ColumnProperty(name="day", type="string"),
                    glue.CfnTable.ColumnProperty(name="hour", type="string"),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{telemetry_bucket_name}/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.openx.data.jsonserde.JsonSerDe",
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="meshId", type="string"),
                        glue.CfnTable.ColumnProperty(name="thingName", type="string"),
                        glue.CfnTable.ColumnProperty(name="event_ts", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="ingestedAt", type="bigint"),
                        glue.CfnTable.ColumnProperty(
                            name="readings",
                            type="array<struct<nodeId:int,humidity:double,raw:int,soil_moisture:double,soil_temperature:double,soil_ph:double,soil_ec:double,soil_nitrogen:double,soil_phosphorus:double,soil_potassium:double,soil_salinity:double,air_temperature:double,air_humidity:double,air_pressure:double,wind_speed:double,rainfall:double,solar_radiation:double,co2_level:double,leaf_wetness:double,pm1:double,pm2_5:double,pm10:double,voc:double,o3_level:double,no2_level:double,so2_level:double,battery_voltage:double,battery_level:double,battery_health:double,signal_strength:int,device_temperature:double,uptime:bigint>>",
                        ),
                    ],
                ),
            ),
        )

        # EXPORTS PARA OTROS STACKS
        self.athena_database = database.ref
        self.athena_output_bucket = athena_output_bucket.bucket_name
        self.telemetry_bucket_name = telemetry_bucket_name
        self.athena_output_bucket = athena_output_bucket
