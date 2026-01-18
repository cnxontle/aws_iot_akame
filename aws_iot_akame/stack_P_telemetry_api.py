from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    aws_lambda as lambda_,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
    aws_apigatewayv2_authorizers as authorizers,
    aws_iam as iam,
)
from constructs import Construct


class TelemetryApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        query_lambda: lambda_.Function,
        user_pool,
        user_pool_client,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # Cognito Authorizer (HTTP API)
        cognito_authorizer = authorizers.HttpJwtAuthorizer(
            "CognitoAuthorizer",
            jwt_issuer=f"https://cognito-idp.{self.region}.amazonaws.com/{user_pool.user_pool_id}",
            jwt_audience=[user_pool_client.user_pool_client_id],
        )

        # HTTP API
        api = apigwv2.HttpApi(
            self,
            "TelemetryHttpApi",
            api_name="telemetry-api",
            description="Telemetry query API (Athena-backed)",
            create_default_stage=False,
        )

        # Lambda integration
        lambda_integration = integrations.HttpLambdaIntegration(
            "QueryIntegration",
            handler=query_lambda,
        )

    
        # Route: GET /telemetry/query
        api.add_routes(
            path="/telemetry/query",
            methods=[apigwv2.HttpMethod.GET],
            integration=lambda_integration,
            authorizer=cognito_authorizer,
        )

        # Throttling (Stage)
        stage = apigwv2.HttpStage(
            self,
            "ProdStage",
            http_api=api,
            stage_name="prod",
            auto_deploy=True,
            throttle=apigwv2.ThrottleSettings(
                rate_limit=10,   # 10 req/seg 
                burst_limit=20,  # picos cortos
            ),
        )

        # Outputs
        CfnOutput(
            self,
            "TelemetryApiUrl",
            value=f"{api.api_endpoint}/prod",
        )

        self.api = api