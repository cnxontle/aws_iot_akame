from aws_cdk import Stack
from constructs import Construct

class InfraStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        # Aquí pondremos IAM roles, CloudWatch logs, etc.
        pass
