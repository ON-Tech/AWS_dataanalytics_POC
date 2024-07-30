from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_kinesisfirehose as firehose,
    aws_apigateway as apigw,
    # aws_sqs as sqs,
)
import json
from constructs import Construct
import pathlib

lambda_transform_code = pathlib.Path(__file__).parent / "functions" / "data_transform"


class PoCDataAnalaticsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Policy to allow writing logs to CloudWatch
        cloudwatch_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=["*"],
            actions=[
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
                "sqs:SendMessage"
            ],
            sid="ReadSQSPolicy"
        )
        
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
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            description="This role enables API Gateway to send streaming data to Amazon Kinesis Data Firehose",
            role_name="APIFirehoseRole",
            inline_policies={
                "APIFirehosePolicy": iam.PolicyDocument(
                    statements=[api_firehose_policy]
                )
            }
            
        )
        APIGateway_Firehose.add_to_policy(cloudwatch_policy)
        
        # Amazon S3 to store streaming data 
        streaming_data_bucket = s3.Bucket(self, "data-streams-firehose-poc-dataanalytics")
        
        # Lambda function that transforms data before Amazon Kinesis Data Firehose ingests it into the object storage bucket.
        transform_data_lambda = _lambda.Function(
            self, 'data_transform_lambda',
            
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler='data_transform_lambda.lambda_handler',
            timeout=Duration.minutes(1),
            code=_lambda.Code.from_asset(str(lambda_transform_code)),
        )



        s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=streaming_data_bucket.bucket_arn,
                compression_format="UNCOMPRESSED",
                role_arn=APIGateway_Firehose.role_arn,
                ),
                



        # # Create the Kinesis Data Firehose delivery stream
        # firehose_delivery_stream=firehose.CfnDeliveryStream(self, "firehose_delivery_stream",
        #     delivery_stream_type="DirectPut",
        #     s3_destination_configuration={
        #         "bucket_arn": streaming_data_bucket.bucket_arn,
        #         "role_arn": APIGateway_Firehose.role_arn,
                

        #         "data_format_conversion_configuration": {
        #             "enabled": True,
        #             "input_format_configuration": {
        #                 "deserializer": {
        #                     "open_x_json_ser_de": {}
        #                 }
        #             },
        #             "output_format_configuration": {
        #                 "serializer": {
        #                     "orc_ser_de": {}
        #                 }
        #             }
        #         },
        #         "processing_configuration": {
        #             "enabled": True,
        #             "processors": [{
        #                 "type": "Lambda",
        #                 "parameters": [{
        #                     "parameter_name": "LambdaArn",
        #                     "parameter_value": transform_data_lambda.function_arn
        #                 }]
        #             }]
        #         }
        #     }
        # )
        
        
        
        
        
        # Kinesis Data Firehose delivery stream to ingest streaming data.
        firehose_delivery_stream = firehose.CfnDeliveryStream(
            self, "firehose_delivery_stream",
            delivery_stream_name="data-analytics-poc-firehose",
            delivery_stream_type="DirectPut",
            s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=streaming_data_bucket.bucket_arn,
                compression_format="UNCOMPRESSED",
                role_arn=APIGateway_Firehose.role_arn,
            
            ),
            # processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
            #         enabled=True,
            #         processors=[
            #             firehose.CfnDeliveryStream.ProcessorProperty(
            #                 type="Lambda",
            #                 parameters=[
            #                     firehose.CfnDeliveryStream.ProcessorParameterProperty(
            #                         parameter_name="LambdaArn",
            #                         parameter_value=transform_data_lambda.function_arn
            #                     )
            #                 ]
            #             )
            #         ]
            #     )
        )
        
        # Grants permissions to s3 to write records from Amazon Kinesis Data Firehose delivery stream.
        s3_write_firehose_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                streaming_data_bucket.bucket_arn,
                f"{streaming_data_bucket.bucket_arn}/*"
            ],
            # principals=firehose_delivery_stream.attr_arn,
            principals= [iam.ArnPrincipal(APIGateway_Firehose.role_arn)],
            actions=[
                "s3:AbortMultipartUpload",
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            sid="StmtID"
        )
        
        # Add the bucket policy to the bucket
        streaming_data_bucket.add_to_resource_policy(s3_write_firehose_policy)
        
        # API Gateway to ingest streaming data
        api = apigw.RestApi(
            self, 'clickstream-ingest-poc',
            rest_api_name='clickstream-ingest-poc',
            deploy_options=apigw.StageOptions(stage_name='prod'),
            
        )
        
        # Create a resource and method for the API
        api_resource = api.root.add_resource("poc")
        
        # Add a POST method to the API resource
        api_resource.add_method(
            "POST",
            apigw.AwsIntegration(
                service="firehose",
                integration_http_method="POST",
                path="firehose/PutRecord",
                options=apigw.IntegrationOptions(
                    credentials_role=APIGateway_Firehose,
                    request_templates={
                        "application/json": json.dumps({
                            "DeliveryStreamName": firehose_delivery_stream.ref,
                            "Record": {
                                "Data": "$util.base64Encode($util.escapeJavaScript($input.json('$')).replace('\'', ''))"
                            }
                        })
                    },
                    integration_responses=[
                        apigw.IntegrationResponse(
                            status_code="200",
                            response_templates={
                                "application/json": "{\"status\":\"OK\"}"
                            }
                        )
                    ]
                )
            )
        )

# policies to allow API Gateway to send streaming data to Amazon Kinesis Data Firehose
        api_firehose_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[APIGateway_Firehose.role_arn],
            actions=[
                "firehose:PutRecord"
            ],
            sid="APIFirehosePolicy"
        )

        # Add the policy to the API Gateway role
        APIGateway_Firehose.add_to_policy(api_firehose_policy)