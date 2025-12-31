from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_iam as iam,
)
from constructs import Construct

class ActivationCodeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, metadata_table, activation_code_table, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        consume_lambda = lambda_.Function(
            self,
            "ConsumeActivationCodeLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/activation_code"),
            timeout=Duration.seconds(10),
            memory_size=128,
            environment={
                "ACTIVATION_CODE_TABLE": activation_code_table.table_name,
                "DEVICE_METADATA_TABLE": metadata_table.table_name,
            },
        )

        activation_code_table.grant_read_write_data(consume_lambda)
        metadata_table.grant_read_write_data(consume_lambda)

        consume_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["iot:UpdateThing", "iot:DescribeThing", "iot:DescribeCertificate", "iot:UpdateCertificate"],
                resources=[
                    f"arn:aws:iot:{self.region}:{self.account}:thing/*",
                    f"arn:aws:iot:{self.region}:{self.account}:cert/*"
                ]
            )
        )

        self.consume_lambda = consume_lambda