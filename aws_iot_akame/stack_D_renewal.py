from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_apigateway as apigw,
    CfnOutput,
    CfnParameter
)
from constructs import Construct

DEFAULT_RENEWAL_DAYS = "30"


class RenewalStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, metadata_table, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        renewal_days_param = CfnParameter(
            self,
            "RenewalDays",
            type="String",
            default=DEFAULT_RENEWAL_DAYS,
            description="Duración de la renovación del dispositivo en días"
        )

        renewal_fn = lambda_.Function(
            self,
            "RenewalLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda/renewal_lambda"),
            timeout=Duration.seconds(15),
            memory_size=256,
            environment={
                "DEVICE_METADATA_TABLE": metadata_table.table_name,
                "RENEWAL_PERIOD_DAYS": renewal_days_param.value_as_string,
            },
        )

        # DynamoDB permissions
        metadata_table.grant_read_write_data(renewal_fn)

        # API Gateway
        api = apigw.RestApi(
            self,
            "RenewalAPI",
            rest_api_name="RenewalAPI",
        )

        for scope_path in ["thing", "user"]:
            scope_res = api.root.add_resource(scope_path)

            for action in ["renew", "revoke", "rehabilitate", "status"]:
                action_res = scope_res.add_resource(action)
                action_res.add_method(
                    "POST",
                    apigw.LambdaIntegration(renewal_fn),
                    authorization_type=apigw.AuthorizationType.IAM,
                )

        CfnOutput(
            self,
            "RenewalApiUrl",
            value=api.url,
            description="Base URL for Renewal API",
        )

        self.api_url = api.url

