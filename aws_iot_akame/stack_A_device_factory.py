from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    Duration
)
from constructs import Construct

class DeviceFactoryStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # DynamoDB Metadata
        metadata_table = dynamodb.Table(
            self, "DeviceMetadata",
            partition_key={"name": "thingName", "type": dynamodb.AttributeType.STRING}
        )

        # Lambda Device Factory
        lambda_fn = lambda_.Function(
            self, "DeviceFactoryLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/device_factory"),
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": metadata_table.table_name
            }
        )

        metadata_table.grant_read_write_data(lambda_fn)
