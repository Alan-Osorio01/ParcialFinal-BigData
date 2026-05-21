#!/usr/bin/env python3
"""
ShopStream PySpark Pipeline — Punto 3
Uso: spark-submit pipeline.py --date 2025-01-15 --raw-bucket shopstream-raw-aad
"""
import argparse
import sys
import os

from cleaning import get_spark, load_events, clean_pipeline
from metrics import (
    metric_top_pages_time,
    metric_bounce_rate,
    metric_conversion_funnel,
    metric_products_view_vs_cart,
    metric_navigation_paths,
    metric_time_by_device_country,
)
from anomaly import detect_session_anomalies


def save_parquet(df, s3_path, partition_cols=None):
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(s3_path)
    print(f"Guardado en: {s3_path}")


def main(date_str: str, raw_bucket: str, processed_bucket: str):
    spark = get_spark("ShopStream-Pipeline")
    spark.sparkContext.setLogLevel("WARN")

    year, month, day = date_str.split("-")
    partition = f"year={year}/month={month}/day={day}"

    raw_path = f"s3://{raw_bucket}/{partition}/events.jsonl"
    tx_path  = f"s3://{raw_bucket}/{partition}/transactions.jsonl"
    out_base = f"s3://{processed_bucket}"

    print(f"Cargando datos de: {raw_path}")
    raw_df          = load_events(spark, raw_path)
    transactions_df = spark.read.json(tx_path)

    print("Aplicando limpieza...")
    clean_df = clean_pipeline(raw_df)
    clean_df.cache()
    total = clean_df.count()
    print(f"Registros limpios: {total:,}")

    print("Calculando métrica 1: Top 20 páginas por tiempo...")
    m1 = metric_top_pages_time(clean_df)
    save_parquet(m1, f"{out_base}/metrics/top_pages_time/{partition}")

    print("Calculando métrica 2: Tasa de rebote...")
    m2 = metric_bounce_rate(clean_df)
    save_parquet(m2, f"{out_base}/metrics/bounce_rate/{partition}")

    print("Calculando métrica 3: Embudo de conversión...")
    m3 = metric_conversion_funnel(clean_df, transactions_df)
    save_parquet(m3, f"{out_base}/metrics/conversion_funnel/{partition}")

    print("Calculando métrica 4: Productos vistos vs carrito...")
    m4 = metric_products_view_vs_cart(clean_df)
    save_parquet(m4, f"{out_base}/metrics/products_view_vs_cart/{partition}")

    print("Calculando métrica 5: Rutas de navegación...")
    m5 = metric_navigation_paths(clean_df)
    save_parquet(m5, f"{out_base}/metrics/navigation_paths/{partition}")

    print("Calculando métrica 6: Tiempo por dispositivo y país...")
    m6 = metric_time_by_device_country(clean_df)
    save_parquet(m6, f"{out_base}/metrics/time_device_country/{partition}")

    print("Detectando anomalías...")
    anomalies = detect_session_anomalies(clean_df)
    save_parquet(anomalies, f"{out_base}/anomalies/{partition}")
    print(f"Anomalías detectadas: {anomalies.count():,}")

    spark.stop()
    print("Pipeline completado exitosamente.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",             required=True, help="YYYY-MM-DD")
    parser.add_argument("--raw-bucket",       default="shopstream-raw-aad")
    parser.add_argument("--processed-bucket", default="shopstream-processed-aad")
    args = parser.parse_args()
    main(args.date, args.raw_bucket, args.processed_bucket)