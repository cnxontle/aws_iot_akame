from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_iot as iot,
    Duration
)
from constructs import Construct

class AuthorizerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        auth_lambda = lambda_.Function(
            self, "AuthLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/authorizer"),
            timeout=Duration.seconds(10)
        )

        iot.CfnAuthorizer(
            self, "AkameAuthorizer",
            authorizer_name="AkameCustomAuth",
            authorizer_function_arn=auth_lambda.function_arn,
            status="ACTIVE"
        )
