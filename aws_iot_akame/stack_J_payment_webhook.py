from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_lambda as lambda_,
    aws_apigateway as apigw,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_ssm as ssm,
)
from constructs import Construct


class StripeWebhookStack(Stack):
    def __init__(self, scope: Construct, id: str, renewal_lambda, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Idempotency table
        idempotency_table = dynamodb.Table(
            self,
            "StripeWebhookIdempotency",
            partition_key={
                "name": "eventId",
                "type": dynamodb.AttributeType.STRING,
            },
            time_to_live_attribute="expiresAt",
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        webhook_fn = lambda_.Function(
            self,
            "StripeWebhookLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/stripe_webhook"),
            timeout=Duration.seconds(10),
            memory_size=256,
            environment={
                "STRIPE_WEBHOOK_SECRET_PARAM": "/stripe/webhook/secret",
                "RENEWAL_LAMBDA_ARN": renewal_lambda.function_arn,
                "IDEMPOTENCY_TABLE": idempotency_table.table_name,
            },
        )

        idempotency_table.grant_write_data(webhook_fn)
        renewal_lambda.grant_invoke(webhook_fn)

        webhook_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter"],
                resources=["*"]
            )
        )

        api = apigw.RestApi(
            self,
            "StripeWebhookAPI",
            rest_api_name="StripeWebhookAPI",
        )

        webhook = api.root.add_resource("stripe")
        webhook.add_method(
            "POST",
            apigw.LambdaIntegration(webhook_fn),
            authorization_type=apigw.AuthorizationType.NONE,
        )

        self.webhook_url = api.url + "stripe"
