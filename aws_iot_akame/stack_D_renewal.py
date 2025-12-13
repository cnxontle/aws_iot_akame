# aws_iot_akame/stack_D_renewal.py
from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as lambda_,
    aws_apigateway as apigw,
)
from constructs import Construct


class RenewalStack(Stack):
    def __init__(self, scope: Construct, id: str, metadata_table, **kwargs):
        super().__init__(scope, id, **kwargs)

        device_admin_fn = lambda_.Function(
            self, "DeviceAdminLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda/renewal_lambda"),
            timeout=Duration.seconds(5),
            memory_size=128,
            environment={
                "DEVICE_METADATA_TABLE": metadata_table.table_name,
            }
        )

        metadata_table.grant_read_write_data(device_admin_fn)

        api = apigw.RestApi(
            self, "DeviceAdminAPI",
            rest_api_name="DeviceAdminAPI"
        )

        for path in ["renew", "revoke", "rehabilitate"]:
            res = api.root.add_resource(path)
            res.add_method(
                "POST",
                apigw.LambdaIntegration(device_admin_fn),
                authorization_type=apigw.AuthorizationType.IAM
            )

        self.api_url = api.url

