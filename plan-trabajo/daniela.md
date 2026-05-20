# Daniela — Procesamiento Distribuido con PySpark
## Punto 3: EMR + PySpark + Métricas de Comportamiento + Anomalías

> **Empieza el Día 2**, cuando Alan suba el primer dataset a S3.
> Pídele a Alan el link del bucket (`s3://shopstream-raw-aad/`) y las credenciales de la cuenta Academy.

---

## Resumen de Responsabilidades

| # | Tarea | Entregable |
|---|---|---|
| 1 | Cluster EMR configurado | Cluster funcional en us-east-1 |
| 2 | Limpieza de datos (dedup, nulos, normalización) | `cleaning.py` |
| 3 | Métrica 1: Top 20 páginas por tiempo promedio | Parquet en S3 processed |
| 4 | Métrica 2: Tasa de rebote por tipo de página | Parquet en S3 processed |
| 5 | Métrica 3: Embudo de conversión | Parquet en S3 processed |
| 6 | Métrica 4: Productos vistos vs agregados al carrito | Parquet en S3 processed |
| 7 | Métrica 5: Top 10 rutas de navegación | Parquet en S3 processed |
| 8 | Métrica 6: Tiempo promedio por dispositivo y país | Parquet en S3 processed |
| 9 | Detección de anomalías (z-score, IQR) | Parquet anomalías en S3 |
| 10 | Notebook Jupyter + script spark-submit | `notebook.ipynb` + `pipeline.py` |
| 11 | Tests unitarios PySpark | `pytest` verde |

---

## DÍA 1 — Configurar Cluster EMR (AWS Academy)

### 1.1 Crear el Cluster EMR desde consola

> AWS Academy: ir a la consola AWS → EMR → Create cluster

**Configuración del cluster:**
- Nombre: `shopstream-emr-cluster`
- Versión: **EMR 6.15.0** (incluye Spark 3.4 + Python 3.10)
- Tipo de aplicaciones: Spark, JupyterEnterpriseGateway
- **Master:** m5.xlarge (1 nodo)
- **Core:** m5.xlarge (2 nodos)
- Key pair: la del laboratorio Academy
- IAM Role para EMR: **EMR_DefaultRole** (ya existe en Academy)
- IAM Role para instancias EC2: **EMR_EC2_DefaultRole** (ya existe en Academy)
- S3 bucket para logs: `s3://shopstream-raw-aad/emr-logs/`

Guardar la configuración en `infra/emr_cluster.json`:

```json
{
  "Name": "shopstream-emr-cluster",
  "ReleaseLabel": "emr-6.15.0",
  "Applications": [
    {"Name": "Spark"},
    {"Name": "JupyterEnterpriseGateway"}
  ],
  "Instances": {
    "MasterInstanceType": "m5.xlarge",
    "SlaveInstanceType": "m5.xlarge",
    "InstanceCount": 3,
    "KeepJobFlowAliveWhenNoSteps": true
  },
  "LogUri": "s3://shopstream-raw-aad/emr-logs/",
  "ServiceRole": "EMR_DefaultRole",
  "JobFlowRole": "EMR_EC2_DefaultRole"
}
```

> **IMPORTANTE:** Los clusters EMR cobran por hora en Academy. Apagar cuando no uses. Recrearlo es rápido con la configuración guardada.

### 1.2 Conectarse al Cluster con EMR Studio Notebook

1. En la consola EMR → Studios → Crear un workspace
2. Adjuntar el cluster `shopstream-emr-cluster`
3. Crear nuevo notebook `shopstream-pipeline.ipynb`

---

## DÍA 2 — Limpieza de Datos en PySpark

### 2.1 Dependencias

En el cluster EMR, instalar en el bootstrap o via pip en el notebook:
```python
# Al inicio del notebook
import subprocess
subprocess.run(["pip", "install", "pandas", "pyarrow"])
```

### 2.2 Script de Limpieza

Crear `punto3_pyspark/transformations/cleaning.py`:

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def get_spark(app_name="ShopStream-Pipeline"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())


def load_events(spark: SparkSession, s3_path: str) -> DataFrame:
    return spark.read.json(s3_path)


def deduplicate(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["event_id"])


def impute_nulls(df: DataFrame) -> DataFrame:
    return (df
        .fillna({"time_on_page_seconds": 0,
                 "results_count": 0,
                 "x_position": 0,
                 "y_position": 0,
                 "referrer": "unknown",
                 "country": "unknown",
                 "device_type": "unknown"})
    )


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
```

---

## DÍA 3 — Métricas 1, 2 y 3

Crear `punto3_pyspark/transformations/metrics.py`:

```python
from pyspark.sql import DataFrame, Window
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
# Sesiones con un único page_view = rebote
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
# page_view → product_view → cart_event → transaction (completed)
def metric_conversion_funnel(df: DataFrame, transactions_df: DataFrame) -> DataFrame:
    date_val = df.select("date").first()["date"]

    users_page_view = df.filter(F.col("event_type") == "page_view").select("user_id").distinct()
    users_product_view = df.filter(F.col("event_type") == "product_view").select("user_id").distinct()
    users_cart = df.filter(F.col("event_type") == "cart_event").select("user_id").distinct()

    completed_txs = transactions_df.filter(
        F.col("status") == "completed"
    ).select("user_id").distinct()

    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    funnel_data = [
        (str(date_val), "1_page_view", users_page_view.count()),
        (str(date_val), "2_product_view", users_product_view.count()),
        (str(date_val), "3_cart_event", users_cart.count()),
        (str(date_val), "4_purchase", completed_txs.count()),
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
        .withColumn("view_to_cart_ratio",
                    F.round(F.col("cart_adds") / (F.col("views") + 0.001), 4))
        .withColumn("high_view_low_cart",
                    F.when(
                        (F.col("views") > 100) & (F.col("view_to_cart_ratio") < 0.05), True
                    ).otherwise(False))
        .orderBy(F.desc("views"))
    )


# MÉTRICA 5: Top 10 rutas de navegación más frecuentes
def metric_navigation_paths(df: DataFrame) -> DataFrame:
    page_views = (df
        .filter(F.col("event_type") == "page_view")
        .select("session_id", "timestamp", "page_url", "date")
        .orderBy("session_id", "timestamp")
    )

    # Crear secuencia de URLs por sesión usando collect_list con orden
    w = Window.partitionBy("session_id").orderBy("timestamp")
    page_views = page_views.withColumn("rank", F.row_number().over(w))

    # Limitar a primeras 5 páginas de cada sesión para las rutas
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
```

---

## DÍA 4 — Métricas 4-6 + Detección de Anomalías

Crear `punto3_pyspark/transformations/anomaly.py`:

```python
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
    std_val = stats["stddev"] or 1

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
    iqr = q3 - q1
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr

    return df.withColumn(
        f"anomaly_iqr_{col_name}",
        (F.col(col_name) < lower) | (F.col(col_name) > upper)
    ).withColumn(f"iqr_lower_{col_name}", F.lit(lower)
    ).withColumn(f"iqr_upper_{col_name}", F.lit(upper))


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

    # Aplicar z-score y IQR al tiempo total
    session_stats = detect_anomalies_zscore(session_stats, "total_time")
    session_stats = detect_anomalies_iqr(session_stats, "total_time")
    session_stats = detect_anomalies_zscore(session_stats, "event_count")

    anomalous = session_stats.filter(
        F.col("anomaly_zscore_total_time") | F.col("anomaly_iqr_total_time") |
        F.col("anomaly_zscore_event_count")
    ).withColumn(
        "anomaly_type",
        F.when(F.col("anomaly_zscore_total_time"), "zscore_time")
         .when(F.col("anomaly_iqr_total_time"), "iqr_time")
         .otherwise("zscore_event_count")
    )

    return anomalous
```

---

## DÍA 4-5 — Pipeline Principal

Crear `punto3_pyspark/pipeline.py` (versión spark-submit + notebook):

```python
#!/usr/bin/env python3
"""
ShopStream PySpark Pipeline — Punto 3
Uso: spark-submit pipeline.py --date 2025-01-15 --raw-bucket shopstream-raw-aad
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from transformations.cleaning import get_spark, load_events, clean_pipeline
from transformations.metrics import (
    metric_top_pages_time,
    metric_bounce_rate,
    metric_conversion_funnel,
    metric_products_view_vs_cart,
    metric_navigation_paths,
    metric_time_by_device_country,
)
from transformations.anomaly import detect_session_anomalies


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
    tx_path = f"s3://{raw_bucket}/{partition}/transactions.jsonl"
    out_base = f"s3://{processed_bucket}"

    print(f"Cargando datos de: {raw_path}")
    raw_df = load_events(spark, raw_path)
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
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--raw-bucket", default="shopstream-raw-aad")
    parser.add_argument("--processed-bucket", default="shopstream-processed-aad")
    args = parser.parse_args()
    main(args.date, args.raw_bucket, args.processed_bucket)
```

### Ejecutar con spark-submit en EMR:

```bash
# SSH al master del cluster (usar la key pair de Academy)
ssh -i ~/Downloads/labsuser.pem hadoop@<MASTER_DNS>

# Subir el script a S3 primero
aws s3 cp punto3_pyspark/ s3://shopstream-raw-aad/scripts/punto3/ --recursive

# Ejecutar
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.adaptive.enabled=true \
  s3://shopstream-raw-aad/scripts/punto3/pipeline.py \
  --date 2025-01-15 \
  --raw-bucket shopstream-raw-aad \
  --processed-bucket shopstream-processed-aad
```

---

## DÍA 5 — Tests Unitarios PySpark

Instalar dependencias locales:
```bash
pip install pyspark==3.4.0 pytest pytest-cov
```

Crear `punto3_pyspark/tests/test_cleaning.py`:

```python
import pytest
from pyspark.sql import SparkSession
import sys
sys.path.insert(0, "punto3_pyspark")
from transformations.cleaning import deduplicate, impute_nulls, filter_valid_events

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_deduplicate(spark):
    data = [
        {"event_id": "1", "event_type": "page_view"},
        {"event_id": "1", "event_type": "page_view"},  # duplicado
        {"event_id": "2", "event_type": "click"},
    ]
    df = spark.createDataFrame(data)
    result = deduplicate(df)
    assert result.count() == 2

def test_filter_valid_events(spark):
    from datetime import datetime
    data = [
        {"event_id": "1", "event_type": "page_view", "user_id": "u1",
         "session_id": "s1", "timestamp": datetime(2025, 1, 15, 10, 0, 0)},
        {"event_id": "2", "event_type": "INVALID", "user_id": "u2",
         "session_id": "s2", "timestamp": datetime(2025, 1, 15, 11, 0, 0)},
        {"event_id": "3", "event_type": "click", "user_id": None,
         "session_id": "s3", "timestamp": datetime(2025, 1, 15, 12, 0, 0)},
    ]
    df = spark.createDataFrame(data)
    result = filter_valid_events(df)
    assert result.count() == 1
```

Crear `punto3_pyspark/tests/test_metrics.py`:

```python
import pytest
from pyspark.sql import SparkSession
from datetime import datetime
import sys
sys.path.insert(0, "punto3_pyspark")
from transformations.metrics import metric_top_pages_time, metric_bounce_rate

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test-metrics").getOrCreate()

@pytest.fixture
def sample_events(spark):
    data = [
        {"event_type": "page_view", "page_url": "/home", "page_type": "home",
         "time_on_page_seconds": 120, "session_id": "s1", "user_id": "u1",
         "date": "2025-01-15", "device_type": "mobile", "country": "CO"},
        {"event_type": "page_view", "page_url": "/home", "page_type": "home",
         "time_on_page_seconds": 60, "session_id": "s2", "user_id": "u2",
         "date": "2025-01-15", "device_type": "desktop", "country": "MX"},
        {"event_type": "page_view", "page_url": "/product/1", "page_type": "product",
         "time_on_page_seconds": 300, "session_id": "s3", "user_id": "u3",
         "date": "2025-01-15", "device_type": "tablet", "country": "CO"},
    ]
    return spark.createDataFrame(data)

def test_top_pages_time_returns_rows(sample_events):
    result = metric_top_pages_time(sample_events)
    assert result.count() > 0
    assert "avg_time_seconds" in result.columns

def test_bounce_rate_range(sample_events):
    result = metric_bounce_rate(sample_events)
    rows = result.collect()
    for row in rows:
        assert 0 <= row["bounce_rate"] <= 100
```

**Ejecutar tests:**
```bash
pytest punto3_pyspark/tests/ -v --cov=punto3_pyspark
```

---

## Checklist Final Daniela

- [ ] Cluster EMR creado y funcional (Spark 3.4 + Python 3.10)
- [ ] Limpieza ejecutada: dedup, nulos, normalización
- [ ] Métrica 1: Top 20 páginas por avg_time_seconds → Parquet en S3
- [ ] Métrica 2: Bounce rate por page_type → Parquet en S3
- [ ] Métrica 3: Embudo de conversión (4 pasos) → Parquet en S3
- [ ] Métrica 4: Productos vistas vs carrito + flag high_view_low_cart → Parquet en S3
- [ ] Métrica 5: Top 10 rutas de navegación → Parquet en S3
- [ ] Métrica 6: Tiempo promedio por device y país → Parquet en S3
- [ ] Anomalías con z-score e IQR → Parquet en S3
- [ ] Notebook `notebook.ipynb` funcional en EMR Studio
- [ ] Script `pipeline.py` ejecutable con spark-submit
- [ ] `pytest` con ≥8 tests verdes
- [ ] **Avisar a Ana** cuando los Parquets estén en `s3://shopstream-processed-aad/`

---

## Comandos Útiles

```bash
# Verificar que llegaron los Parquets de métricas
aws s3 ls s3://shopstream-processed-aad/metrics/ --recursive

# Ver schema de un Parquet
python3 -c "import pyarrow.parquet as pq; print(pq.read_schema('s3://shopstream-processed-aad/metrics/top_pages_time/year=2025/month=01/day=15/'))"

# Ejecutar pipeline local (para pruebas, con datos pequeños)
spark-submit punto3_pyspark/pipeline.py --date 2025-01-15

# SSH al master EMR
ssh -i ~/Downloads/labsuser.pem hadoop@<EMR_MASTER_DNS>

# Monitorear job en YARN
yarn application -list
yarn logs -applicationId <app_id>

# Correr tests locales
pytest punto3_pyspark/tests/ -v
```

---

## Notas Importantes

- **Caché:** usar `df.cache()` después del clean_pipeline para no releer S3 en cada métrica.
- **Adaptive Query Execution:** activar `spark.sql.adaptive.enabled=true` para optimizar joins.
- **Parquet particionado:** todos los outputs llevan la partición `year=/month=/day=` para que Glue los detecte automáticamente.
- **Apagar el cluster EMR** cuando termines la sesión de trabajo (cuesta por hora en Academy).
