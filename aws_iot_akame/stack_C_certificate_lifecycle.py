from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
)
from constructs import Construct


class CertificateLifecycleStack(Stack):
    def __init__(self, scope: Construct, id: str, metadata_table, **kwargs):
        super().__init__(scope, id, **kwargs)

        lifecycle_fn = lambda_.Function(
            self,
            "CertificateLifecycleLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/certificate_lifecycle"),
            timeout=Duration.seconds(300),
            memory_size=256,
            environment={
                "DEVICE_METADATA_TABLE": metadata_table.table_name,
                "LIFECYCLE_GSI": "ByStatusExpiry"
            }
        )

        metadata_table.grant_read_data(lifecycle_fn)

        lifecycle_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "iot:UpdateCertificate",
                    "iot:DescribeCertificate",
                ],
                resources=[
                    f"arn:aws:iot:{self.region}:{self.account}:cert/*"
                ]
            )
        )

        events.Rule(
            self,
            "CertificateLifecycleSchedule",
            schedule=events.Schedule.rate(Duration.minutes(30)),
            targets=[targets.LambdaFunction(lifecycle_fn)]
        )