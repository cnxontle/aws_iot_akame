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

                    # PARTITION PROJECTION
                    "projection.enabled": "true",

                    "projection.year.type": "string",
                    "projection.year.range": "2023,2100",

                    "projection.month.type": "string",
                    "projection.month.range": "01,12",
                    "projection.month.digits": "2",

                    "projection.day.type": "string",
                    "projection.day.range": "01,31",
                    "projection.day.digits": "2",

                    "projection.hour.type": "string",
                    "projection.hour.range": "00,23",
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
                            type="""
                            array<
                                struct<
                                    nodeId:int,
                                    metrics:map<string,double>
                                >
                            >
                            """.strip(),
                        ),
                    ],
                ),
            ),
        )

        # EXPORTS PARA OTROS STACKS
        self.athena_database = database.ref
        self.athena_output_bucket = athena_output_bucket.bucket_name
        self.telemetry_bucket_name = telemetry_bucket_name
