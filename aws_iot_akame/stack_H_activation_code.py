from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
)
from constructs import Construct


class ActivationCodeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, metadata_table, **kwargs):

        super().__init__(scope, construct_id, **kwargs)

         # =========================
        # DynamoDB: Activation Codes
        # =========================
        activation_code_table = dynamodb.Table(
            self,
            "ActivationCodeTable",
            partition_key={
                "name": "code",
                "type": dynamodb.AttributeType.STRING,
            },
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # =========================
        # Admin Lambda
        # =========================
        admin_lambda = lambda_.Function(
            self,
            "ActivationCodeAdminLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/activation_code"),
            timeout=Duration.seconds(10),
            memory_size=128,
            environment={
                "ACTIVATION_CODE_TABLE": activation_code_table.table_name,
                "DEFAULT_CODE_TTL_SECONDS": str(7 * 24 * 3600),
            },
        )

        activation_code_table.grant_read_write_data(admin_lambda)

        # =========================
        # Consume Lambda
        # =========================
        consume_lambda = lambda_.Function(
            self,
            "ConsumeActivationCodeLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="consume_handler.main",
            code=lambda_.Code.from_asset("lambda/activation_code"),
            timeout=Duration.seconds(5),
            memory_size=128,
            environment={
                "ACTIVATION_CODE_TABLE": activation_code_table.table_name,
                "DEVICE_METADATA_TABLE": metadata_table.table_name,  # OK
            },
        )

        activation_code_table.grant_read_write_data(consume_lambda)

        # =========================
        # Exports internos
        # =========================
        self.activation_code_table = activation_code_table
        self.admin_lambda = admin_lambda
        self.consume_lambda = consume_lambda
