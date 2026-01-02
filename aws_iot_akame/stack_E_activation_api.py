from aws_cdk import (
    Stack,
    aws_apigatewayv2 as apigw,
    aws_apigatewayv2_integrations as integrations,
    aws_apigatewayv2_authorizers as authorizers,
)
from constructs import Construct


class ActivationApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        consume_lambda,
        user_pool,
        user_pool_client,
        **kwargs
    ):
        super().__init__(scope, construct_id, **kwargs)

        # HTTP API
        http_api = apigw.HttpApi(
            self,
            "ActivationHttpApi",
            api_name="akame-activation-api",
        )

        # Cognito JWT Authorizer
        jwt_authorizer = authorizers.HttpJwtAuthorizer(
            "CognitoAuthorizer",
            jwt_issuer=f"https://cognito-idp.{self.region}.amazonaws.com/{user_pool.user_pool_id}",
            jwt_audience=[user_pool_client.user_pool_client_id],
        )

        # Lambda integration
        lambda_integration = integrations.HttpLambdaIntegration(
            "ConsumeActivationCodeIntegration",
            handler=consume_lambda,
        )

        # Route protegida
        http_api.add_routes(
            path="/activate",
            methods=[apigw.HttpMethod.POST],
            integration=lambda_integration,
            authorizer=jwt_authorizer,
        )

        self.http_api = http_api

