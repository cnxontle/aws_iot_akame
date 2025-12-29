from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_apigateway as apigw,
    aws_ssm as ssm,
)
from constructs import Construct

class CheckoutSessionStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Lambda
        checkout_fn = lambda_.Function(
            self,
            "CreateCheckoutSessionLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda/create_checkout_session"),
            timeout=Duration.seconds(10),
            memory_size=256,
            environment={
                "STRIPE_SECRET_PARAM": "/stripe/secret_key",  # almacena tu secret key en SSM
            },
        )

        checkout_fn.add_to_role_policy(
            statement=checkout_fn.role.add_managed_policy(
                policy_arn="arn:aws:iam::aws:policy/AmazonSSMReadOnlyAccess"
            )
        )

        # API Gateway
        api = apigw.RestApi(
            self,
            "CheckoutAPI",
            rest_api_name="CheckoutAPI",
        )

        create_checkout_res = api.root.add_resource("create-checkout-session")
        create_checkout_res.add_method(
            "POST",
            apigw.LambdaIntegration(checkout_fn),
            authorization_type=apigw.AuthorizationType.NONE,
        )

        self.api_url = api.url
