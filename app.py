#!/usr/bin/env python3
import aws_cdk as cdk
import os

from aws_iot_akame.stack_A_device_factory import DeviceFactoryStack
from aws_iot_akame.stack_B_authorizer import AuthorizerStack
from aws_iot_akame.stack_D_renewal import RenewalStack
from aws_iot_akame.stack_G_cognito import CognitoStack
from aws_iot_akame.stack_H_activation_code import ActivationCodeStack
from aws_iot_akame.stack_I_ingestion import TelemetryIngestionStack

app = cdk.App()

env = cdk.Environment(
    account=os.getenv("AWS_ACCOUNT_ID"),
    region=os.getenv("us-east-2")
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


    env=env
)

# Módulo I
ingestion = TelemetryIngestionStack(
    app,
    "TelemetryIngestionStack",
    metadata_table=factory.metadata_table,   # PASA LA TABLA
    env=env
)


app.synth()

   