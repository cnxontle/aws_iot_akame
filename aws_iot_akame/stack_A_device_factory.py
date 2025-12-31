from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_iot as iot,
    Duration,
    RemovalPolicy
)
from constructs import Construct


class DeviceFactoryStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        gateway_thing_type = iot.CfnThingType(
            self,
            "GatewayThingType",
            thing_type_name="Gateway"
        )

        gateway_policy = iot.CfnPolicy(
            self,
            "GatewayPolicy",
            policy_name="GatewayBasePolicy",
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "iot:Connect",
                        "Resource": "arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "iot:Publish",
                            "iot:Receive"
                        ],
                        "Resource": [
                            "arn:aws:iot:*:*:topic/gateway/${iot:Connection.Thing.Attributes[userId]}/data/telemetry"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": "iot:Subscribe",
                        "Resource": [
                            "arn:aws:iot:*:*:topicfilter/gateway/${iot:Connection.Thing.Attributes[userId]}/command/*"
                        ]
                    }
                ]
            }
        )

        # DynamoDB Metadata Table
        metadata_table = dynamodb.Table(
            self,
            "DeviceMetadata",
            partition_key={
                "name": "thingName",
                "type": dynamodb.AttributeType.STRING,
            },
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        metadata_table.add_global_secondary_index(
            index_name="ByUser",
            partition_key={
                "name": "userId",
                "type": dynamodb.AttributeType.STRING,
            },
            projection_type=dynamodb.ProjectionType.ALL,
        )

        metadata_table.add_global_secondary_index(
            index_name="ByLifecycleBucket",
            partition_key=dynamodb.Attribute(name="lifecycleBucket", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="expiresAt", type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=["thingName", "certificateId"],
        )


        activation_code_table = dynamodb.Table(
            self,
            "ActivationCodeTable",
            partition_key=dynamodb.Attribute(name="code", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )


        # Lambda Device Factory
        lambda_fn = lambda_.Function(
            self,
            "DeviceFactoryLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda/device_factory"),
            timeout=Duration.seconds(30),
            environment={
                "METADATA_TABLE": metadata_table.table_name,
                "ACTIVATION_CODE_TABLE": activation_code_table.table_name,
                "DEFAULT_EXPIRATION_SECONDS": str(3 * 24 * 3600),
            }
        )

        lambda_fn.node.add_dependency(gateway_thing_type)
        lambda_fn.node.add_dependency(gateway_policy)

        # --- IAM IoT permissions ---
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "iot:CreateKeysAndCertificate",
                ],
                resources=["*"],
            )
        )
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "iot:CreateThing",
                    "iot:DeleteThing",
                    "iot:AttachThingPrincipal",
                    "iot:DetachThingPrincipal",
                    "iot:AttachPolicy",
                    "iot:DetachPolicy",
                    "iot:UpdateCertificate",
                    "iot:DeleteCertificate",
                    "iot:DescribeCertificate",
                ],
                resources=[
                    f"arn:aws:iot:{self.region}:{self.account}:thing/*",
                    f"arn:aws:iot:{self.region}:{self.account}:cert/*",
                    f"arn:aws:iot:{self.region}:{self.account}:policy/GatewayBasePolicy",
                ],
            )
        )



        # --- DynamoDB permissions ---
        metadata_table.grant_read_write_data(lambda_fn)
        activation_code_table.grant_read_write_data(lambda_fn)

        #--- Store references ---
        self.metadata_table = metadata_table
        self.activation_code_table = activation_code_table
        self.lambda_fn = lambda_fn
