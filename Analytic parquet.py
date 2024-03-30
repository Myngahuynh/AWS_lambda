import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog 1
AWSGlueDataCatalog1_node1708933035675 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="cleaned-data-json-to-parquetyoutube", transformation_ctx="AWSGlueDataCatalog1_node1708933035675")

# Script generated for node AWS Glue Data Catalog 2
AWSGlueDataCatalog2_node1708933042712 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="data_cleaned_csv_to_parquetyoutube_csv_to_parquet", transformation_ctx="AWSGlueDataCatalog2_node1708933042712")

# Script generated for node Join
Join_node1708933501898 = Join.apply(frame1=AWSGlueDataCatalog2_node1708933042712, frame2=AWSGlueDataCatalog1_node1708933035675, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1708933501898")

# Script generated for node analytic_target_parquet
analytic_target_parquet_node1708997676740 = glueContext.getSink(path="s3://de-on-youtube-analytic-parquet", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["category_id"], enableUpdateCatalog=True, transformation_ctx="analytic_target_parquet_node1708997676740")
analytic_target_parquet_node1708997676740.setCatalogInfo(catalogDatabase="db_youtube_analytics",catalogTableName="final_analytics")
analytic_target_parquet_node1708997676740.setFormat("glueparquet", compression="snappy")
analytic_target_parquet_node1708997676740.writeFrame(Join_node1708933501898)
job.commit()