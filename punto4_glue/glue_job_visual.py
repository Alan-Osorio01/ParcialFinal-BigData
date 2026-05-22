import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Source_S3_top_pages
Source_S3_top_pages_node1779420938867 = glueContext.create_dynamic_frame.from_catalog(database="shopstream_glue_db", table_name="top_pages_time", transformation_ctx="Source_S3_top_pages_node1779420938867")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1779421053878_ruleset = """
    Rules = [
        IsComplete "page_url",
        IsComplete "date",
        ColumnValues "avg_time_seconds" >= 0,
        ColumnValues "total_views" >= 0,
        ColumnValues "bounce_rate" between 0 and 100
    ]
"""

EvaluateDataQuality_node1779421053878 = EvaluateDataQuality().process_rows(frame=Source_S3_top_pages_node1779420938867, ruleset=EvaluateDataQuality_node1779421053878_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779421053878", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node originalData
originalData_node1779421150103 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1779421053878, key="originalData", transformation_ctx="originalData_node1779421150103")

# Script generated for node Change Schema
ChangeSchema_node1779421256539 = ApplyMapping.apply(frame=originalData_node1779421150103, mappings=[("page_url", "string", "page_url", "string"), ("date", "date", "date", "date"), ("avg_time_seconds", "double", "avg_time_seconds", "decimal"), ("total_views", "long", "total_views", "long")], transformation_ctx="ChangeSchema_node1779421256539")

# Escribir a RDS PostgreSQL
connection_options = {
    "url": "jdbc:postgresql://shopstream-rds-dw.cavmfs9savk3.us-east-1.rds.amazonaws.com:5432/postgres",
    "dbtable": "shopstream.fact_page_metrics",
    "user": "Admin_postgres",
    "password": "parcialfinal3",
    "driver": "org.postgresql.Driver"
}

glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1779421256539,
    connection_type="jdbc",
    connection_options=connection_options,
    transformation_ctx="write_to_rds"
)

job.commit()