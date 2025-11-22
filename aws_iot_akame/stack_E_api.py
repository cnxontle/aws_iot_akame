from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_apigateway as api,
    aws_cognito as cognito,
    Duration
)
from constructs import Construct

class ApiStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        user_pool = cognito.UserPool(self, "UserPool")

        api_lambda = lambda_.Function(
            self, "ApiLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/api"),
            timeout=Duration.seconds(10)
        )

        rest_api = api.LambdaRestApi(
            self, "AkameApi",
            handler=api_lambda
        )
