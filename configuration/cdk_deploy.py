from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,

    aws_lambda as _lambda,
    aws_iam as iam,
    aws_dynamodb as dynamodb,
    aws_s3_notifications as s3n,
    RemovalPolicy,
)
from constructs import Construct

BUCKET_NAME = "birdtag-models-fit5225-g138"
DDB_NAME = "bird-db"

class BirdTagStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # S3 Bucket
        bird_bucket = s3.Bucket(
            self, "birdBucket",
            bucket_name=BUCKET_NAME,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # create prefix
        for prefix in ["models/", "images/", "videos/", "audios/", "thumbs/"]:
            s3_deploy.BucketDeployment(
                self, f"Init{prefix}",
                destination_bucket=bird_bucket,
                destination_key_prefix=prefix,
                sources=[s3_deploy.Source.data("placeholder.txt", "init")],
            )

        # DynamoDB Table
        bird_table = dynamodb.Table(
                self, "BirdTable",
                table_name="bird-db",
                partition_key=dynamodb.Attribute(name="media_id", type=dynamodb.AttributeType.STRING),
                sort_key=dynamodb.Attribute(name="bird_tag", type=dynamodb.AttributeType.STRING),
                billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
                removal_policy=RemovalPolicy.DESTROY,
        )

        # GSI: bird_tag-index (project All)
        bird_table.add_global_secondary_index(
                index_name="bird_tag-index",
                partition_key=dynamodb.Attribute(name="bird_tag", type=dynamodb.AttributeType.STRING),
                projection_type=dynamodb.ProjectionType.ALL
        )

        # GSI: thumb_url-index (project Include: bird_tag, file_type, full_url)
        bird_table.add_global_secondary_index(
                index_name="thumb_url-index",
                partition_key=dynamodb.Attribute(name="thumb_url", type=dynamodb.AttributeType.STRING),
                projection_type=dynamodb.ProjectionType.INCLUDE,
                non_key_attributes=["bird_tag", "file_type", "full_url"]
        )

        # GSI: full_url-index (project Include: bird_tag, file_type, thumb_url)
        bird_table.add_global_secondary_index(
                index_name="full_url-index",
                partition_key=dynamodb.Attribute(name="full_url", type=dynamodb.AttributeType.STRING),
                projection_type=dynamodb.ProjectionType.INCLUDE,
                non_key_attributes=["bird_tag", "file_type", "thumb_url"]
        )
