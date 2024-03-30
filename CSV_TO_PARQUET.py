import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# from awsglue.dynamic_frame import Dynamic_frame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1708915879574 = glueContext.create_dynamic_frame.from_catalog(
    database="de_youtube_raw",
    table_name="video",
    transformation_ctx="AmazonS3_node1708915879574",
)

# Script generated for node de-on-youtube-csv-to-par
deonyoutubecsvtopar_node1708916132112 = ApplyMapping.apply(
    frame=AmazonS3_node1708915879574,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "bigint"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "bigint"),
        ("likes", "long", "likes", "bigint"),
        ("dislikes", "long", "dislikes", "bigint"),
        ("comment_count", "long", "comment_count", "bigint"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
    ],
    transformation_ctx="deonyoutubecsvtopar_node1708916132112",
)

# Script generated for node target_s3_csv_to_parquet
target_s3_csv_to_parquet_node1708916507205 = (
    glueContext.write_dynamic_frame.from_options(
        frame=deonyoutubecsvtopar_node1708916132112,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": "s3://de-on-youtubecleaned-ap-dev/youtube_csv_to_parquet/",
            "partitionKeys": [],
        },
        format_options={"compression": "snappy"},
        transformation_ctx="target_s3_csv_to_parquet_node1708916507205",
    )
)

job.commit()
