from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def get_spark(app_name="ShopStream-Pipeline"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())


def load_events(spark: SparkSession, s3_path: str) -> DataFrame:
    return spark.read.json(s3_path)


def deduplicate(df: DataFrame) -> DataFrame:
    if "event_id" in df.columns:
        return df.dropDuplicates(["event_id"])
    return df.dropDuplicates()


def impute_nulls(df: DataFrame) -> DataFrame:
    defaults = {
        "time_on_page_seconds": 0,
        "results_count": 0,
        "x_position": 0,
        "y_position": 0,
        "referrer": "unknown",
        "country": "unknown",
        "device_type": "unknown"
    }
    # Solo imputar columnas que existan en el DataFrame
    existing = {k: v for k, v in defaults.items() if k in df.columns}
    return df.fillna(existing)


def normalize_timestamps(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp"))
    ).withColumn(
        "date",
        F.to_date(F.col("timestamp"))
    )


def filter_valid_events(df: DataFrame) -> DataFrame:
    valid_types = ["page_view", "click", "search", "product_view", "cart_event"]
    return df.filter(
        F.col("event_type").isin(valid_types) &
        F.col("timestamp").isNotNull() &
        F.col("user_id").isNotNull() &
        F.col("session_id").isNotNull()
    )


def normalize_text_fields(df: DataFrame) -> DataFrame:
    if "country" in df.columns:
        df = df.withColumn("country", F.upper(F.trim(F.col("country"))))
    if "device_type" in df.columns:
        df = df.withColumn("device_type", F.lower(F.trim(F.col("device_type"))))
    if "page_type" in df.columns:
        df = df.withColumn("page_type", F.lower(F.trim(F.col("page_type"))))
    return df


def clean_pipeline(df: DataFrame) -> DataFrame:
    df = deduplicate(df)
    df = filter_valid_events(df)
    df = impute_nulls(df)
    df = normalize_timestamps(df)
    df = normalize_text_fields(df)
    return df