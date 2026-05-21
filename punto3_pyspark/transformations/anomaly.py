from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def detect_anomalies_zscore(df: DataFrame, col_name: str, threshold: float = 3.0) -> DataFrame:
    """Detecta valores con |z-score| > threshold."""
    stats = df.select(
        F.mean(col_name).alias("mean"),
        F.stddev(col_name).alias("stddev")
    ).collect()[0]

    mean_val = stats["mean"] or 0
    std_val  = stats["stddev"] or 1

    return df.withColumn(
        f"zscore_{col_name}",
        F.abs((F.col(col_name) - mean_val) / std_val)
    ).withColumn(
        f"anomaly_zscore_{col_name}",
        F.col(f"zscore_{col_name}") > threshold
    )


def detect_anomalies_iqr(df: DataFrame, col_name: str, factor: float = 1.5) -> DataFrame:
    """Detecta outliers usando rango intercuartílico (IQR)."""
    quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
    q1, q3 = quantiles[0], quantiles[1]
    iqr     = q3 - q1
    lower   = q1 - factor * iqr
    upper   = q3 + factor * iqr

    return (df
        .withColumn(
            f"anomaly_iqr_{col_name}",
            (F.col(col_name) < lower) | (F.col(col_name) > upper)
        )
        .withColumn(f"iqr_lower_{col_name}", F.lit(lower))
        .withColumn(f"iqr_upper_{col_name}", F.lit(upper))
    )


def detect_session_anomalies(df: DataFrame) -> DataFrame:
    """Detecta sesiones anómalas por tiempo total o número de eventos."""
    page_views = df.filter(F.col("event_type") == "page_view")

    session_stats = (page_views
        .groupBy("session_id", "user_id", "date")
        .agg(
            F.sum("time_on_page_seconds").alias("total_time"),
            F.count("*").alias("event_count")
        )
    )

    # Aplicar z-score e IQR al tiempo total y z-score al conteo de eventos
    session_stats = detect_anomalies_zscore(session_stats, "total_time")
    session_stats = detect_anomalies_iqr(session_stats,    "total_time")
    session_stats = detect_anomalies_zscore(session_stats, "event_count")

    anomalous = session_stats.filter(
        F.col("anomaly_zscore_total_time") |
        F.col("anomaly_iqr_total_time")    |
        F.col("anomaly_zscore_event_count")
    ).withColumn(
        "anomaly_type",
        F.when(F.col("anomaly_zscore_total_time"),  "zscore_time")
         .when(F.col("anomaly_iqr_total_time"),      "iqr_time")
         .otherwise("zscore_event_count")
    )

    return anomalous