from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_kms as kms,
    aws_s3 as s3,
    aws_iam as iam,
    aws_iot as iot,
    aws_lambda as _lambda,
    aws_kinesisfirehose as firehose,
)
from constructs import Construct


class TelemetryIngestionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # 1. S3 Bucket
        telemetry_bucket = s3.Bucket(
            self,
            "TelemetryRawBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # 2. Lambda para validar meshId y crear prefix dinámico
        transform_lambda = _lambda.Function(
            self,
            "TelemetryTransformLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="transform.handler",
            timeout=Duration.seconds(30),
            code=_lambda.Code.from_asset("lambda/transform"),
            environment={
                "DYNAMO_TABLE": "DeviceFactoryStack-DeviceMetadataA",
            },
        )

        # Permisos DynamoDB
        transform_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:GetItem", "dynamodb:Query"],
                resources=["*"],  # Puedes restringir si quieres
            )
        )

        # 3. Firehose role
        firehose_role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "lambda:InvokeFunction",
                    "lambda:GetFunctionConfiguration",
                ],
                resources=[transform_lambda.function_arn],
            )
        )


        telemetry_bucket.grant_write(firehose_role)
        transform_lambda.grant_invoke(firehose_role)

        # KMS Key for Firehose S3 encryption
        my_kms_key = kms.Key(
            self,
            "TelemetryFirehoseKMSKey",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # 4. Firehose Delivery Stream con Lambda Transform
        delivery_stream = firehose.CfnDeliveryStream(
            self,
            "TelemetryFirehose",
            delivery_stream_type="DirectPut",
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=telemetry_bucket.bucket_arn,
                role_arn=firehose_role.role_arn,
                compression_format="GZIP",
                prefix="meshId=!{partitionKeyFromLambda:meshId}/year=!{timestamp:yyyy}/",
                error_output_prefix="errors/!{firehose:error-output-type}/",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=300,
                    size_in_m_bs=64,
                ),
                encryption_configuration=firehose.CfnDeliveryStream.EncryptionConfigurationProperty(
                    kms_encryption_config=firehose.CfnDeliveryStream.KMSEncryptionConfigProperty(
                        awskms_key_arn=my_kms_key.key_arn
                    )
                ),
                processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                    enabled=True,
                    processors=[
                        firehose.CfnDeliveryStream.ProcessorProperty(
                            type="Lambda",
                            parameters=[
                                firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="LambdaArn",
                                    parameter_value=transform_lambda.function_arn,
                                )
                            ],
                        )
                    ],
                ),
                dynamic_partitioning_configuration=firehose.CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                    enabled=True
                ),
            ),
        )


        # 5. IoT Rule IAM Role
        iot_rule_role = iam.Role(
            self,
            "IoTRuleRole",
            assumed_by=iam.ServicePrincipal("iot.amazonaws.com"),
        )

        iot_rule_role.add_to_policy(
            iam.PolicyStatement(
                actions=["firehose:PutRecord", "firehose:PutRecordBatch"],
                resources=[delivery_stream.attr_arn],
            )
        )

        # 6. IoT Rule → Firehose
        iot.CfnTopicRule(
            self,
            "GatewayTelemetryRule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                aws_iot_sql_version="2016-03-23",
                sql="""
                SELECT
                    *,
                    topic(3) AS meshId,
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

        CfnOutput(
            self,
            "TelemetryRawBucketName",
            value=telemetry_bucket.bucket_name,
            export_name="TelemetryRawBucketName"
        )

        CfnOutput(self, "FirehoseName", value=delivery_stream.ref)
        self.telemetry_bucket = telemetry_bucket
        self.firehose_stream = delivery_stream

