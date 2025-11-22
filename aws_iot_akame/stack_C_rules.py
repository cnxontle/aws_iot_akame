from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
    aws_iot as iot,
)
from constructs import Construct

class IotRulesStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        telemetry_table = dynamodb.Table(
            self, "TelemetryTable",
            partition_key={"name": "UserID", "type": dynamodb.AttributeType.STRING},
            sort_key={"name": "Timestamp", "type": dynamodb.AttributeType.STRING}
        )

        # IoT Rule
        iot.CfnTopicRule(
            self, "HumidityRule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql="SELECT * FROM 'users/+/devices/+/humidity'",
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        dynamo_dbv2=iot.CfnTopicRule.DynamoDBv2ActionProperty(
                            put_item=iot.CfnTopicRule.PutItemInputProperty(
                                table_name=telemetry_table.table_name
                            ),
                            role_arn="arn:aws:iam::123456789012:role/service-role/iot-dynamo-role"
                        )
                    )
                ],
                rule_disabled=False
            )
        )
