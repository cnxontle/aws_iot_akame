from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_dynamodb as dynamodb,
    aws_logs as logs,
    aws_s3 as s3,
)
from constructs import Construct
from typing import Union


class TelemetryQueryStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        metadata_table: dynamodb.ITable,
        athena_database: str,
        athena_output_bucket: Union[s3.IBucket, str],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ─────────────────────────────────────────────
        # Normalizar bucket (string o IBucket)
        # ─────────────────────────────────────────────
        if isinstance(athena_output_bucket, str):
            output_bucket = s3.Bucket.from_bucket_name(
                self,
                "AthenaOutputBucket",
                athena_output_bucket,
            )
            output_bucket_name = athena_output_bucket
        else:
            output_bucket = athena_output_bucket
            output_bucket_name = athena_output_bucket.bucket_name

        # ─────────────────────────────────────────────
        # Lambda de query
        # ─────────────────────────────────────────────
        query_lambda = lambda_.Function(
            self,
            "TelemetryQueryLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/telemetry_query"),
            timeout=Duration.seconds(30),
            memory_size=1024,
            log_retention=logs.RetentionDays.ONE_WEEK,
            environment={
                "METADATA_TABLE": metadata_table.table_name,
                "ATHENA_DATABASE": athena_database,
                "ATHENA_OUTPUT": f"s3://{output_bucket_name}/",
                "ATHENA_WORKGROUP": "telemetry-prod",
            },
        )

        # DynamoDB (GSI ByUser)
        metadata_table.grant_read_data(query_lambda)

        # Athena
        query_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=[f"arn:aws:athena:{self.region}:{self.account}:workgroup/telemetry-prod"],
            )
        )

        # S3 resultados Athena
        output_bucket.grant_read_write(query_lambda)

        # Exponer lambda
        self.lambda_function = query_lambda
