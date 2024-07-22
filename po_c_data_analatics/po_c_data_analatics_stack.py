from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_kinesisfirehose as firehose,
    # aws_sqs as sqs,
)

from constructs import Construct
import pathlib

lambda_transform_code = pathlib.Path(__file__).parent / "functions" /"data_transform"''


class PoCDataAnalaticsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Grants permissions to write records to Amazon Kinesis Data Firehose delivery stream.
        api_firehose_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=["*"],
            actions=[
                "firehose:PutRecord"
            ],
            sid="APIFirehosePolicy"
        )
        
        # IAM role that enables API Gateway to send streaming data to Amazon Kinesis Data Firehose
        APIGateway_Firehose = iam.Role(
            self, "APIFirehoseRole",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
            description="This role enables API Gateway to send streaming data to Amazon Kinesis Data Firehose",
            role_name="APIFirehoseRole",
            inline_policies={
                "APIFirehosePolicy": iam.PolicyDocument(
                    statements=[api_firehose_policy]
              )}
        )
        
        # Amazon S3 to store streaming data 
        streaming_data_bucket = s3.Bucket(self, "data-streams-firehose-poc-dataanalytics")
        
        
        # Lambda function that transforms data before Amazon Kinesis Data Firehose ingests it into the object storage bucket.
        transform_data_lambda = _lambda.Function(
            self, 'data_transform_lambda',
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler='data_transform_lambda.lambda_handler',
            timeout=Duration.seconds(10),
            code=_lambda.Code.from_asset(str(lambda_transform_code)),
        )

        # Kinesis Data Firehose delivery stream to ingest streaming data.
        firehose_delivery_stream = firehose.CfnDeliveryStream(
            self, "firehose_delivery_stream",
            delivery_stream_name="data-analytics-poc-firehose",
            delivery_stream_type="DirectPut",
            s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=streaming_data_bucket.bucket_arn,
                compression_format="UNCOMPRESSED",
                role_arn=APIGateway_Firehose.role_arn
            )
        )