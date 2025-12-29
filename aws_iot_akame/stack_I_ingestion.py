from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_iot as iot,
    aws_iam as iam,
    Duration
)
from constructs import Construct


class TelemetryIngestionStack(Stack):
    def __init__(self, scope: Construct, id: str, metadata_table, **kwargs):
        super().__init__(scope, id, **kwargs)

        validator_fn = lambda_.Function(
            self,
            "TelemetryValidator",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda/telemetry_validator"),
            timeout=Duration.seconds(5),
            environment={
                "DEVICE_METADATA_TABLE": metadata_table.table_name
            }
        )

        metadata_table.grant_read_data(validator_fn)

        iot.CfnTopicRule(
            self,
            "GatewayTelemetryRule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql="""
                SELECT
                  *,
                  clientid() AS clientid,
                  topic() AS topic
                FROM 'gateway/+/data/telemetry'
                """,
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        lambda_=iot.CfnTopicRule.LambdaActionProperty(
                            function_arn=validator_fn.function_arn
                        )
                    )
                ]
            )
        )

        validator_fn.add_permission(
            "AllowIoTInvoke",
            principal=iam.ServicePrincipal("iot.amazonaws.com"),
            action="lambda:InvokeFunction"
        )
