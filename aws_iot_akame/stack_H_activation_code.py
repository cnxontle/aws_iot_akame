from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
)
from constructs import Construct


class ActivationCodeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Tabla de códigos de activación
        activation_code_table = dynamodb.Table(
            self,
            "ActivationCodeTable",
            partition_key={
                "name": "code",
                "type": dynamodb.AttributeType.STRING,
            },
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expiresAt",
            removal_policy=RemovalPolicy.RETAIN,
        )

        # Lambda administrativa (uso interno)
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
                "DEFAULT_CODE_TTL_SECONDS": str(7 * 24 * 3600),  # 7 días
            },
        )

        # Permisos mínimos necesarios
        activation_code_table.grant_read_write_data(admin_lambda)

        self.activation_code_table = activation_code_table
        self.admin_lambda = admin_lambda
