from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    Duration
)
from constructs import Construct

class RenewalStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        lambda_.Function(
            self, "RenewalLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/renewal"),
            timeout=Duration.seconds(10)
        )
