from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_iam as iam,
)
from constructs import Construct


class TelemetryAggregatesApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        metadata_table_name: str,
        athena_database: str,
        athena_output_bucket: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # Lambda
        aggregates_lambda = lambda_.Function(
            self,
            "TelemetryAggregatesLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/telemetry_aggregates"),
            timeout=Duration.seconds(60),
            memory_size=512,
            environment={
                "METADATA_TABLE": metadata_table_name,
                "ATHENA_DATABASE": athena_database,
                "ATHENA_OUTPUT": f"s3://{athena_output_bucket}/",
            },
        )

        # DynamoDB permissions (query metadata)
        aggregates_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:Query",
                ],
                resources=[
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{metadata_table_name}",
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{metadata_table_name}/index/*",
                ],
            )
        )

        # Athena permissions
        aggregates_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=["*"],
            )
        )

        # S3 output for Athena results
        aggregates_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:PutObject",
                    "s3:GetObject",
                ],
                resources=[
                    f"arn:aws:s3:::{athena_output_bucket}/*",
                ],
            )
        )

        self.lambda_function = aggregates_lambda
