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
            description="Duración de la renovación del dispositivo en días."
        )

        
        device_admin_fn = lambda_.Function(
            self,
            "DeviceAdminLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda/renewal_lambda"),
            timeout=Duration.seconds(10),
            memory_size=256,
            environment={
                "DEVICE_METADATA_TABLE": metadata_table.table_name,
                # Pasamos el valor del parámetro (en días) a la Lambda
                "RENEWAL_PERIOD_DAYS": renewal_days_param.value_as_string, 
            },
        )

        # Permisos DynamoDB
        metadata_table.grant_read_write_data(device_admin_fn)

        # API Gateway
        api = apigw.RestApi(
            self,
            "DeviceAdminAPI",
            rest_api_name="DeviceAdminAPI",
        )

        # Rutas: /thing/* y /user/*
        for scope_path in ["thing", "user"]:
            scope_res = api.root.add_resource(scope_path)

            for action in ["renew", "revoke", "rehabilitate", "status"]:
                action_res = scope_res.add_resource(action)
                action_res.add_method(
                    "POST",
                    apigw.LambdaIntegration(device_admin_fn),
                    authorization_type=apigw.AuthorizationType.IAM,
                )
        
        CfnOutput(
            self,
            "DeviceAdminApiUrl",
            value=api.url,
            description="Base URL for Device Admin API",
        )

        self.api_url = api.url
