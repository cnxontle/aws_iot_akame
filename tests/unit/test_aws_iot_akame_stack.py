import aws_cdk as core
import aws_cdk.assertions as assertions

from aws_iot_akame.stack_A_device_factory import AwsIotAkameStack

# example tests. To run these tests, uncomment this file along with the example
# resource in aws_iot_akame/aws_iot_akame_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AwsIotAkameStack(app, "aws-iot-akame")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
