from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql import functions as F


# MÉTRICA 1: Top 20 páginas con mayor tiempo de permanencia promedio
def metric_top_pages_time(df: DataFrame) -> DataFrame:
    page_views = df.filter(F.col("event_type") == "page_view")
    return (page_views
        .groupBy("page_url", "date")
        .agg(
            F.avg("time_on_page_seconds").alias("avg_time_seconds"),
            F.count("*").alias("total_views")
        )
        .orderBy(F.desc("avg_time_seconds"))
        .limit(20)
    )


# MÉTRICA 2: Tasa de rebote por tipo de página
def metric_bounce_rate(df: DataFrame) -> DataFrame:
    page_views = df.filter(F.col("event_type") == "page_view")

    session_page_counts = (page_views
        .groupBy("session_id", "page_type", "date")
        .agg(F.count("*").alias("page_count"))
    )

    bounced = session_page_counts.withColumn(
        "is_bounce", F.when(F.col("page_count") == 1, 1).otherwise(0)
    )

    return (bounced
        .groupBy("page_type", "date")
        .agg(
            F.count("*").alias("total_sessions"),
            F.sum("is_bounce").alias("bounced_sessions")
        )
        .withColumn(
            "bounce_rate",
            F.round(F.col("bounced_sessions") / F.col("total_sessions") * 100, 2)
        )
        .orderBy("page_type")
    )


# MÉTRICA 3: Embudo de conversión
def metric_conversion_funnel(df: DataFrame, transactions_df: DataFrame) -> DataFrame:
    date_val = df.select("date").first()["date"]

    users_page_view = df.filter(F.col("event_type") == "page_view").select("user_id").distinct()
    users_product_view = df.filter(F.col("event_type") == "product_view").select("user_id").distinct()
    users_cart = df.filter(F.col("event_type") == "cart_event").select("user_id").distinct()

    completed_txs = transactions_df.filter(
        F.col("status") == "completed"
    ).select("user_id").distinct()

    spark = SparkSession.getActiveSession()

    funnel_data = [
        (str(date_val), "1_page_view",     users_page_view.count()),
        (str(date_val), "2_product_view",  users_product_view.count()),
        (str(date_val), "3_cart_event",    users_cart.count()),
        (str(date_val), "4_purchase",      completed_txs.count()),
    ]

    return spark.createDataFrame(funnel_data, ["date", "funnel_step", "user_count"])


# MÉTRICA 4: Productos vistos vs agregados al carrito
def metric_products_view_vs_cart(df: DataFrame) -> DataFrame:
    product_views = (df
        .filter(F.col("event_type") == "product_view")
        .groupBy("product_id", "category", "date")
        .agg(F.count("*").alias("views"))
    )

    cart_adds = (df
        .filter((F.col("event_type") == "cart_event") & (F.col("action") == "add"))
        .groupBy("product_id", "date")
        .agg(F.count("*").alias("cart_adds"))
    )

    return (product_views
        .join(cart_adds, on=["product_id", "date"], how="left")
        .fillna({"cart_adds": 0})
        .withColumn(
            "view_to_cart_ratio",
            F.round(F.col("cart_adds") / (F.col("views") + 0.001), 4)
        )
        .withColumn(
            "high_view_low_cart",
            F.when(
                (F.col("views") > 100) & (F.col("view_to_cart_ratio") < 0.05), True
            ).otherwise(False)
        )
        .orderBy(F.desc("views"))
    )


# MÉTRICA 5: Top 10 rutas de navegación más frecuentes
def metric_navigation_paths(df: DataFrame) -> DataFrame:
    page_views = (df
        .filter(F.col("event_type") == "page_view")
        .select("session_id", "timestamp", "page_url", "date")
    )

    w = Window.partitionBy("session_id").orderBy("timestamp")
    page_views = page_views.withColumn("rank", F.row_number().over(w))
    page_views_limited = page_views.filter(F.col("rank") <= 5)

    session_paths = (page_views_limited
        .groupBy("session_id", "date")
        .agg(F.concat_ws(" → ", F.collect_list("page_url")).alias("path"))
    )

    return (session_paths
        .groupBy("path", "date")
        .agg(F.count("*").alias("frequency"))
        .orderBy(F.desc("frequency"))
        .limit(10)
    )


# MÉTRICA 6: Tiempo promedio por dispositivo y por país
def metric_time_by_device_country(df: DataFrame) -> DataFrame:
    page_views = df.filter(F.col("event_type") == "page_view")
    return (page_views
        .groupBy("device_type", "country", "date")
        .agg(
            F.avg("time_on_page_seconds").alias("avg_time_seconds"),
            F.count("*").alias("total_views"),
            F.stddev("time_on_page_seconds").alias("stddev_time")
        )
        .orderBy("device_type", "country")
    )