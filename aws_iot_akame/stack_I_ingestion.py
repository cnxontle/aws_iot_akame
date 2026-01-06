from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_iam as iam,
    aws_iot as iot,
    aws_kinesisfirehose as firehose,
)
from constructs import Construct


class TelemetryIngestionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # 1. S3 Bucket (RAW telemetry)
        telemetry_bucket = s3.Bucket(
            self,
            "TelemetryRawBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(365 * 3)  # 3 años
                )
            ],
        )

        # 2. Firehose IAM Role
        firehose_role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )

        telemetry_bucket.grant_write(firehose_role)


        # 3. Kinesis Firehose → S3
        delivery_stream = firehose.CfnDeliveryStream(
            self,
            "TelemetryFirehose",
            delivery_stream_type="DirectPut",
            s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=telemetry_bucket.bucket_arn,
                role_arn=firehose_role.role_arn,
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60,
                    size_in_m_bs=5,
                ),
                compression_format="GZIP",
                prefix=(
                    "year=!{timestamp:yyyy}/"
                    "month=!{timestamp:MM}/"
                    "day=!{timestamp:dd}/"
                    "hour=!{timestamp:HH}/"
                ),
                error_output_prefix="errors/!{firehose:error-output-type}/",
            ),
        )


        # 4. IoT Rule IAM Role
        iot_rule_role = iam.Role(
            self,
            "IoTRuleRole",
            assumed_by=iam.ServicePrincipal("iot.amazonaws.com"),
        )

        iot_rule_role.add_to_policy(
            iam.PolicyStatement(
                actions=["firehose:PutRecord",
                         "firehose:PutRecordBatch",
                         ],
                resources=[delivery_stream.attr_arn],
            )
        )


        # 5. IoT Rule (MQTT → Firehose)
        iot.CfnTopicRule(
            self,
            "GatewayTelemetryRule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                aws_iot_sql_version="2016-03-23",
                sql="""
                SELECT
                    *,
                    topic(3) AS thingName,
                    timestamp() AS ingestedAt
                FROM 'gateway/data/telemetry/+'
                """,
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        firehose=iot.CfnTopicRule.FirehoseActionProperty(
                            delivery_stream_name=delivery_stream.ref,
                            role_arn=iot_rule_role.role_arn,
                            separator="\n",
                        )
                    )
                ],
                rule_disabled=False,
            ),
        )


        self.telemetry_bucket = telemetry_bucket
        self.firehose_stream = delivery_stream
