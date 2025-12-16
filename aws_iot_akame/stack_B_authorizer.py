# aws_iot_akame/stack_B_authorizer.py
from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as lambda_,
    aws_iot as iot,
)
from constructs import Construct

class AuthorizerStack(Stack):
    def __init__(self, scope: Construct, id: str, metadata_table, **kwargs):
        super().__init__(scope, id, **kwargs)


        auth_fn = lambda_.Function(
            self, "AuthLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda/auth_lambda"),
            timeout=Duration.seconds(5),
            memory_size=128,
            
            environment={
                "DEVICE_METADATA_TABLE": metadata_table.table_name,
            }
        )

        metadata_table.grant_read_data(auth_fn)

        authorizer = iot.CfnAuthorizer(
            self,
            "CustomGatewayAuthorizer",
            authorizer_name="GatewayAuthorizer",
            authorizer_function_arn=auth_fn.function_arn,
            signing_disabled=True,
            status="ACTIVE",
                    
        )
        self.authorizer_name = authorizer.authorizer_name
