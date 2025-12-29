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
            index_name="ByActivationCode",
            partition_key={
                "name": "activationCode",
                "type": dynamodb.AttributeType.STRING,
            },
            projection_type=dynamodb.ProjectionType.ALL,
        )


        metadata_table.add_global_secondary_index(
            index_name="ByStatusExpiry",
            partition_key=dynamodb.Attribute(
                name="lifecycleStatus",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="expiresAt",
                type=dynamodb.AttributeType.NUMBER,
            ),
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=[
                "thingName",
                "certificateId",
                "certificateArn",
                "status"
            ],
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
                "TABLE_NAME": metadata_table.table_name,
                "DEFAULT_EXPIRATION_SECONDS": str(30 * 24 * 3600),  # 30 días
            }
        )

        lambda_fn.node.add_dependency(gateway_thing_type)

        # --- IAM IoT permissions ---
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "iot:CreateThing",
                    "iot:CreateKeysAndCertificate",
                    "iot:AttachThingPrincipal",
                    "iot:AttachPolicy",
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
        self.gateway_policy = gateway_policy
        self.metadata_table = metadata_table
