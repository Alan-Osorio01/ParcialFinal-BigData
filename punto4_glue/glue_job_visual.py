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

JDBC_URL = "jdbc:postgresql://shopstream-rds-dw.cavmfs9savk3.us-east-1.rds.amazonaws.com:5432/postgres"
JDBC_OPTS = {
    "url": JDBC_URL,
    "user": "Admin_postgres",
    "password": "parcialfinal3",
    "driver": "org.postgresql.Driver",
}

# ══════════════════════════════════════════════════════════════════
# BLOQUE 1 — top_pages_time → shopstream.fact_page_metrics
# ══════════════════════════════════════════════════════════════════
source_pages = glueContext.create_dynamic_frame.from_catalog(
    database="shopstream_glue_db",
    table_name="top_pages_time",
    transformation_ctx="source_pages",
)

dq_ruleset = """
    Rules = [
        IsComplete "page_url",
        IsComplete "date",
        ColumnValues "avg_time_seconds" >= 0,
        ColumnValues "total_views" >= 0,
        ColumnValues "bounce_rate" between 0 and 100
    ]
"""
dq_result = EvaluateDataQuality().process_rows(
    frame=source_pages,
    ruleset=dq_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "dq_pages",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

pages_clean = SelectFromCollection.apply(
    dfc=dq_result, key="originalData", transformation_ctx="pages_clean"
)

pages_mapped = ApplyMapping.apply(
    frame=pages_clean,
    mappings=[
        ("page_url",         "string", "page_url",         "string"),
        ("date",             "date",   "date",             "date"),
        ("avg_time_seconds", "double", "avg_time_seconds", "decimal"),
        ("total_views",      "long",   "total_views",      "long"),
    ],
    transformation_ctx="pages_mapped",
)

glueContext.write_dynamic_frame.from_options(
    frame=pages_mapped,
    connection_type="jdbc",
    connection_options={**JDBC_OPTS, "dbtable": "shopstream.fact_page_metrics"},
    transformation_ctx="write_page_metrics",
)

# ══════════════════════════════════════════════════════════════════
# BLOQUE 2 — time_device_country → shopstream.fact_session_summary
# ══════════════════════════════════════════════════════════════════
source_sessions = glueContext.create_dynamic_frame.from_catalog(
    database="shopstream_glue_db",
    table_name="time_device_country",
    transformation_ctx="source_sessions",
)

sessions_mapped = ApplyMapping.apply(
    frame=source_sessions,
    mappings=[
        ("device_type",      "string", "device_type",      "string"),
        ("country",          "string", "country",          "string"),
        ("date",             "date",   "date",             "date"),
        ("avg_time_seconds", "double", "avg_time_seconds", "decimal"),
        ("total_views",      "bigint", "total_views",      "long"),
        ("stddev_time",      "double", "stddev_time",      "decimal"),
    ],
    transformation_ctx="sessions_mapped",
)

glueContext.write_dynamic_frame.from_options(
    frame=sessions_mapped,
    connection_type="jdbc",
    connection_options={**JDBC_OPTS, "dbtable": "shopstream.fact_session_summary"},
    transformation_ctx="write_session_summary",
)

# ══════════════════════════════════════════════════════════════════
# BLOQUE 3 — anomalies → shopstream.fact_anomalies
# ══════════════════════════════════════════════════════════════════
source_anomalies = glueContext.create_dynamic_frame.from_catalog(
    database="shopstream_glue_db",
    table_name="anomalies",
    transformation_ctx="source_anomalies",
)

anomalies_mapped = ApplyMapping.apply(
    frame=source_anomalies,
    mappings=[
        ("session_id",        "string", "session_id",  "string"),
        ("user_id",           "string", "user_id",     "string"),
        ("date",              "date",   "date",         "date"),
        ("total_time",        "bigint", "total_time",   "decimal"),
        ("event_count",       "bigint", "event_count",  "long"),
        ("zscore_total_time", "double", "zscore_time",  "decimal"),
        ("anomaly_type",      "string", "anomaly_type", "string"),
    ],
    transformation_ctx="anomalies_mapped",
)

glueContext.write_dynamic_frame.from_options(
    frame=anomalies_mapped,
    connection_type="jdbc",
    connection_options={**JDBC_OPTS, "dbtable": "shopstream.fact_anomalies"},
    transformation_ctx="write_anomalies",
)

job.commit()
