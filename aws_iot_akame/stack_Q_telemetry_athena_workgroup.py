from aws_cdk import (
    Stack,
    aws_athena as athena,
    aws_s3 as s3,
)
from constructs import Construct


class TelemetryAthenaWorkGroupStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        athena_output_bucket: s3.IBucket,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        self.workgroup = athena.CfnWorkGroup(
            self,
            "TelemetryWorkGroup",
            name="telemetry-prod",
            description="WorkGroup for telemetry queries with cost guardrails",
            state="ENABLED",
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,  # NO overrides
                publish_cloud_watch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query= 1 * 1024 * 1024 * 1024,  # 1 GB
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_output_bucket.bucket_name}/",
                ),
            ),
        )
