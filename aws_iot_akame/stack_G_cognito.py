from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    aws_cognito as cognito,
)
from constructs import Construct



class CognitoStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # User Pool (usuarios)
        user_pool = cognito.UserPool(
            self,
            "AkameUserPool",
            user_pool_name="akame-user-pool",
            self_sign_up_enabled=True,

            # Login por email
            sign_in_aliases=cognito.SignInAliases(
                email=True
            ),

            # Políticas de password (puedes ajustarlas)
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_digits=True,
                require_lowercase=True,
                require_uppercase=False,
                require_symbols=False,
            ),

            # Auto-verificación
            auto_verify=cognito.AutoVerifiedAttrs(
                email=True
            ),

            # Recuperación de cuenta
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,

            # Mantener la tabla al borrar el stack
            removal_policy=RemovalPolicy.RETAIN

        )

        # App Client (Android / frontend)
        user_pool_client = cognito.UserPoolClient(
            self,
            "AkameUserPoolClient",
            user_pool=user_pool,
            user_pool_client_name="akame-android-client",

            # Flujo típico para apps móviles
            auth_flows=cognito.AuthFlow(
                user_password=True,
                user_srp=True,
            ),

            # No usar secret en apps móviles
            generate_secret=False,
        )

        # Outputs (para otros stacks / frontend)
        CfnOutput(
            self,
            "UserPoolId",
            value=user_pool.user_pool_id,
        )

        CfnOutput(
            self,
            "UserPoolClientId",
            value=user_pool_client.user_pool_client_id,
        )

        # Exponer como atributos si otros stacks los importan
        self.user_pool = user_pool
        self.user_pool_client = user_pool_client
