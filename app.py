#!/usr/bin/env python3
import aws_cdk as cdk
import os

from aws_iot_akame.stack_A_device_factory import DeviceFactoryStack
from aws_iot_akame.stack_B_authorizer import AuthorizerStack
from aws_iot_akame.stack_C_certificate_lifecycle import CertificateLifecycleStack
from aws_iot_akame.stack_D_renewal import RenewalStack
from aws_iot_akame.stack_G_cognito import CognitoStack
from aws_iot_akame.stack_H_activation_code import ActivationCodeStack
from aws_iot_akame.stack_E_activation_api import ActivationApiStack
from aws_iot_akame.stack_I_ingestion import TelemetryIngestionStack
from aws_iot_akame.stack_L_telemetry_analytics import TelemetryAnalyticsStack
from aws_iot_akame.stack_M_telemetry_query import TelemetryQueryStack
from aws_iot_akame.stack_N_telemetry_athena_view import TelemetryAthenaViewsStack
from aws_iot_akame.stack_O_telemetry_aggregates_api import TelemetryAggregatesApiStack
from aws_iot_akame.stack_P_telemetry_api import TelemetryApiStack


app = cdk.App()

env = cdk.Environment(
    account=os.getenv("AWS_ACCOUNT_ID"),
    region=os.getenv("AWS_REGION", "us-east-2")
)

# Módulo A
factory= DeviceFactoryStack(
    app,
    "DeviceFactoryStack",
    env=env  
)

# Módulo B
authorizer = AuthorizerStack(
    app,
    "AuthorizerStack",
    metadata_table=factory.metadata_table,   # PASA LA TABLA
    env=env
)

# Módulo C
certificate_lifecycle = CertificateLifecycleStack(
    app,
    "CertificateLifecycleStack",
    metadata_table=factory.metadata_table,   # PASA LA TABLA
    env=env
)

# Módulo D
renewal = RenewalStack(
    app,
    "RenewalStack",
    metadata_table=factory.metadata_table,   # PASA LA TABLA
    env=env
)


# Módulo G
cognito = CognitoStack(
    app,
    "CognitoStack",
    env=env
)

# Módulo H
activation_code = ActivationCodeStack(
    app,
    "ActivationCodeStack",
    metadata_table=factory.metadata_table,   # PASA LA TABLA
    activation_code_table=factory.activation_code_table,  # PASA LA TABLA
    env=env
)

# Módulo E
activation_api = ActivationApiStack(
    app,
    "ActivationApiStack",
    consume_lambda=activation_code.consume_lambda,  
    user_pool=cognito.user_pool,                      
    user_pool_client=cognito.user_pool_client,        
    env=env
)

# Módulo I
telemetry_ingestion = TelemetryIngestionStack(  
    app,
    "TelemetryIngestionStack",
    env=env
)

# Módulo L
telemetry_analytics = TelemetryAnalyticsStack(
    app,
    "TelemetryAnalyticsStack",
    telemetry_bucket_name=telemetry_ingestion.telemetry_bucket.bucket_name,
    env=env
)

# Módulo M
telemetry_query = TelemetryQueryStack(
    app,
    "TelemetryQueryStack",
    metadata_table=factory.metadata_table,  
    athena_database=telemetry_analytics.athena_database,
    athena_output_bucket=telemetry_analytics.athena_output_bucket,
    env=env
)

# Módulo N
telemetry_athena_views = TelemetryAthenaViewsStack(
    app,
    "TelemetryAthenaViewsStack",
    athena_database=telemetry_analytics.athena_database,
    athena_output_bucket=telemetry_analytics.athena_output_bucket,
    env=env
)

# Módulo O
telemetry_aggregates_api = TelemetryAggregatesApiStack(
    app,
    "TelemetryAggregatesApiStack",
    metadata_table_name=factory.metadata_table.table_name,
    athena_database=telemetry_analytics.athena_database,
    athena_output_bucket=telemetry_analytics.athena_output_bucket,
    env=env
)

# Módulo P
telemetry_api = TelemetryApiStack(
    app,
    "TelemetryApiStack",
    query_lambda=telemetry_query.lambda_function,
    user_pool=cognito.user_pool,
    user_pool_client=cognito.user_pool_client,
    env=env
)

app.synth()

   