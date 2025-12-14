from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    Duration,
    RemovalPolicy
)
from constructs import Construct

class DeviceFactoryStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # DynamoDB Metadata
        metadata_table = dynamodb.Table(
            self, "DeviceMetadata",
            partition_key={"name": "thingName", "type": dynamodb.AttributeType.STRING},
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN
        )

        metadata_table.add_global_secondary_index(
            index_name="ByUser",
            partition_key={"name": "userId", "type": dynamodb.AttributeType.STRING},
            projection_type=dynamodb.ProjectionType.ALL
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

        # IAM Role for Lambda to interact with IoT
        iot_policy = iam.PolicyStatement(
            actions=[
                "iot:CreateThing",
                "iot:CreateKeysAndCertificate",
                "iot:CreatePolicy",
                "iot:AttachPolicy",
                "iot:AttachThingPrincipal",
                # Opcional: Para poder obtener/verificar si una política ya existe
                "iot:GetPolicy" 
            ],
            resources=["*"] # Para IoT, se suele usar "*" para muchas acciones
        )
        
        # 3. Adjunta la política al rol de la Lambda
        lambda_fn.add_to_role_policy(iot_policy)

        # Permiso para DynamoDB 
        metadata_table.grant_read_write_data(lambda_fn)

        self.metadata_table = metadata_table