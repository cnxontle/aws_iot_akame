#!/usr/bin/env python3
import aws_cdk as cdk
import os

from aws_iot_akame.stack_A_device_factory import DeviceFactoryStack

app = cdk.App()

env = cdk.Environment(
    account=os.getenv("052247097248"),
    region=os.getenv("us-east-2")
)

DeviceFactoryStack(
    app,
    "DeviceFactoryStack",
    env=env  
)

app.synth()

   