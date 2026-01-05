from aws_cdk import (
    Stack,
    CustomResource, 
    aws_lambda as lambda_,
    aws_iam as iam,
    custom_resources as cr,
    Duration,
)
from constructs import Construct


class TelemetryAthenaViewsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        athena_database: str,
        athena_output_bucket: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        view_lambda = lambda_.Function(
            self,
            "CreateTelemetryViewsLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/athena_views"),
            timeout=Duration.seconds(60),
            environment={
                "ATHENA_DATABASE": athena_database,
                "ATHENA_OUTPUT": f"s3://{athena_output_bucket}/",
            },
        )

        view_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:CreateTable",
                ],
                resources=["*"],
            )
        )

        view_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[f"arn:aws:s3:::{athena_output_bucket}/*"],
            )
        )

        provider = cr.Provider(
            self,
            "AthenaViewProvider",
            on_event_handler=view_lambda,
        )

        CustomResource(
            self,
            "TelemetryAthenaViews",
            service_token=provider.service_token,
        )

        self.view_lambda = view_lambda
