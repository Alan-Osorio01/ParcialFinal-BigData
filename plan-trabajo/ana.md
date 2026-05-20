# Ana — Datawarehouse + Glue Studio + API REST
## Punto 4 (RDS + Glue + Triggers) + API Lambda con Zappa

> **Empieza el Día 1** con el diseño del DW y RDS.
> Espera a que Daniela suba Parquets a `s3://shopstream-processed-aad/` (Día 5) para conectar el Glue ETL real.
> Para la API puedes usar datos mock de RDS desde el Día 4.

---

## Resumen de Responsabilidades

| # | Tarea | Entregable |
|---|---|---|
| 1 | RDS PostgreSQL + schema DW | Base de datos funcional en AWS |
| 2 | Script SQL schema DW | `punto4_glue/dw_schema.sql` |
| 3 | Glue Studio ETL Visual (S3 → RDS) | Job funcional con nodos gráficos |
| 4 | Nodo Data Quality en Glue Studio | Reglas de validación configuradas |
| 5 | Trigger condicional ETL → carga RDS | Workflow con condición SUCCEEDED |
| 6 | Trigger programado 2:00 AM UTC | Schedule en Glue Workflow |
| 7 | Flask app + Zappa setup | `api/app.py` + `zappa_settings.json` |
| 8 | Endpoint GET /pages/top | `api/handlers/pages.py` |
| 9 | Endpoint GET /sessions/summary | `api/handlers/sessions.py` |
| 10 | Endpoint GET /anomalies | `api/handlers/anomalies.py` |
| 11 | Tests unitarios API | `pytest` verde |

---

## DÍA 1 — RDS PostgreSQL (Datawarehouse)

### 1.1 Crear instancia RDS en AWS Academy

> AWS Academy: ir a consola RDS → Create database

**Configuración:**
- Método: Standard create
- Engine: **PostgreSQL 15**
- Template: **Free tier**
- Nombre identificador: `shopstream-rds-dw`
- Master username: `shopstream_admin`
- Master password: (guardar en un .env que NO va a GitHub)
- Clase de instancia: `db.t3.micro`
- Storage: 20 GB gp2
- **Public access: Yes** (para conectarse desde EMR y Lambda en Academy)
- VPC Security Group: crear uno nuevo `shopstream-sg` que permita:
  - Puerto 5432 desde 0.0.0.0/0 (en Academy con la VPC default)

Guardar el **Endpoint** en `infra/rds_endpoint.txt`.

### 1.2 Schema del Datawarehouse

Crear `punto4_glue/dw_schema.sql`:

```sql
-- ============================================================
-- ShopStream DataWarehouse Schema
-- PostgreSQL 15 — Star Schema para analítica de comportamiento
-- ============================================================

CREATE SCHEMA IF NOT EXISTS shopstream;

-- Tabla de dimensión: páginas
CREATE TABLE IF NOT EXISTS shopstream.dim_pages (
    page_id      SERIAL PRIMARY KEY,
    page_url     TEXT NOT NULL UNIQUE,
    page_type    VARCHAR(50)
);

-- Tabla de dimensión: dispositivos y países
CREATE TABLE IF NOT EXISTS shopstream.dim_device_country (
    dc_id        SERIAL PRIMARY KEY,
    device_type  VARCHAR(20) NOT NULL,
    country      CHAR(2) NOT NULL,
    UNIQUE (device_type, country)
);

-- Tabla de dimensión: productos
CREATE TABLE IF NOT EXISTS shopstream.dim_products (
    product_id   TEXT PRIMARY KEY,
    category     VARCHAR(100),
    price        NUMERIC(10, 2)
);

-- ============================================================
-- Tablas de hechos (fact tables) — métricas calculadas
-- ============================================================

-- Fact: Métricas de páginas por día
CREATE TABLE IF NOT EXISTS shopstream.fact_page_metrics (
    id              SERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    page_url        TEXT NOT NULL,
    avg_time_seconds NUMERIC(10, 2),
    total_views     BIGINT,
    bounce_rate     NUMERIC(5, 2),
    page_type       VARCHAR(50),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Resumen de sesiones por dispositivo/país/día
CREATE TABLE IF NOT EXISTS shopstream.fact_session_summary (
    id              SERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    device_type     VARCHAR(20),
    country         CHAR(2),
    avg_time_seconds NUMERIC(10, 2),
    total_views     BIGINT,
    stddev_time     NUMERIC(10, 2),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Embudo de conversión por día
CREATE TABLE IF NOT EXISTS shopstream.fact_conversion_funnel (
    id              SERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    funnel_step     VARCHAR(50),
    user_count      BIGINT,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Productos vistos vs carrito
CREATE TABLE IF NOT EXISTS shopstream.fact_product_performance (
    id                  SERIAL PRIMARY KEY,
    date                DATE NOT NULL,
    product_id          TEXT NOT NULL,
    category            VARCHAR(100),
    views               BIGINT,
    cart_adds           BIGINT,
    view_to_cart_ratio  NUMERIC(8, 4),
    high_view_low_cart  BOOLEAN,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Rutas de navegación top 10
CREATE TABLE IF NOT EXISTS shopstream.fact_navigation_paths (
    id          SERIAL PRIMARY KEY,
    date        DATE NOT NULL,
    path        TEXT NOT NULL,
    frequency   BIGINT,
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Anomalías detectadas
CREATE TABLE IF NOT EXISTS shopstream.fact_anomalies (
    id              SERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    session_id      TEXT NOT NULL,
    user_id         TEXT,
    total_time      NUMERIC(10, 2),
    event_count     BIGINT,
    zscore_time     NUMERIC(8, 4),
    anomaly_type    VARCHAR(50),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para performance de la API
CREATE INDEX IF NOT EXISTS idx_page_metrics_date ON shopstream.fact_page_metrics(date);
CREATE INDEX IF NOT EXISTS idx_session_summary_date ON shopstream.fact_session_summary(date);
CREATE INDEX IF NOT EXISTS idx_anomalies_date ON shopstream.fact_anomalies(date);
CREATE INDEX IF NOT EXISTS idx_page_metrics_page_url ON shopstream.fact_page_metrics(page_url);
CREATE INDEX IF NOT EXISTS idx_session_summary_device ON shopstream.fact_session_summary(device_type, country);
```

**Aplicar schema:**
```bash
# Instalar psycopg2
pip install psycopg2-binary

# Conectar y ejecutar
psql -h <RDS_ENDPOINT> -U shopstream_admin -d postgres -f punto4_glue/dw_schema.sql

# O en Python
python3 -c "
import psycopg2
conn = psycopg2.connect(
    host='<RDS_ENDPOINT>',
    user='shopstream_admin',
    password='<PASSWORD>',
    dbname='postgres'
)
conn.autocommit = True
cur = conn.cursor()
with open('punto4_glue/dw_schema.sql') as f:
    cur.execute(f.read())
print('Schema creado.')
conn.close()
"
```

---

## DÍA 2-3 — Glue Studio ETL Visual

### 2.1 Crear Glue Database y Crawlers

En la consola AWS Glue:

1. **Databases** → Add database → Nombre: `shopstream_glue_db`

2. **Crawlers** → Add crawler:
   - Nombre: `shopstream-processed-crawler`
   - Data source: S3 → `s3://shopstream-processed-aad/metrics/`
   - IAM Role: **AWSGlueServiceRole** (usar LabRole en Academy)
   - Target database: `shopstream_glue_db`
   - Schedule: On demand
   - Ejecutar el crawler para que detecte los Parquets de Daniela

### 2.2 Crear Job ETL en Glue Studio (Visual — OBLIGATORIO)

> Este es el único job que **debe** hacerse visualmente con el editor de nodos de Glue Studio.

**Ir a AWS Glue → ETL Jobs → Visual ETL:**

**Nodos que crear (de izquierda a derecha en el grafo):**

```
[S3 Source: top_pages_time]
          ↓
[Data Quality Check]
          ↓
[ApplyMapping / Transform]
          ↓
[PostgreSQL Target: fact_page_metrics]
```

**Configuración de cada nodo:**

**Nodo 1 — S3 Source:**
- Type: Amazon S3
- Format: Parquet
- Path: `s3://shopstream-processed-aad/metrics/top_pages_time/`
- Database: `shopstream_glue_db`
- Table: `top_pages_time` (detectada por el crawler)

**Nodo 2 — Data Quality (OBLIGATORIO):**
- Type: Evaluate Data Quality
- Ruleset (escribir en el editor de reglas):
```
Rules = [
    IsComplete "page_url",
    IsComplete "date",
    ColumnValues "avg_time_seconds" >= 0,
    ColumnValues "total_views" >= 0,
    ColumnValues "bounce_rate" between 0 and 100
]
```
- Action if fails: Send to separate output (para capturar registros malos)

**Nodo 3 — ApplyMapping:**
- Mapear columnas de Parquet → columnas de RDS:

| Parquet | RDS | Tipo |
|---|---|---|
| page_url | page_url | string → string |
| date | date | string → date |
| avg_time_seconds | avg_time_seconds | double → decimal |
| total_views | total_views | long → bigint |

**Nodo 4 — PostgreSQL Target (JDBC):**
- Type: PostgreSQL
- JDBC URL: `jdbc:postgresql://<RDS_ENDPOINT>:5432/postgres`
- Username: `shopstream_admin`
- Password: (usar Glue Connection con secreto)
- Table: `shopstream.fact_page_metrics`
- Write mode: Append

**Guardar y publicar el job.** Exportar el JSON del job y guardarlo en `punto4_glue/glue_job_visual.json` (desde el menú "Export job" del Studio).

### 2.3 Crear Glue Connection para RDS

En Glue → Connections → Add connection:
- Nombre: `shopstream-rds-connection`
- Type: JDBC
- JDBC URL: `jdbc:postgresql://<RDS_ENDPOINT>:5432/postgres`
- Username: `shopstream_admin`
- Password: tu password de RDS

---

## DÍA 3 — Triggers y Workflow en Glue

### 3.1 Crear el Workflow

En Glue → Workflows → Create workflow:
- Nombre: `shopstream-daily-workflow`

### 3.2 Trigger Programado (Schedule 2:00 AM UTC)

En el workflow → Add trigger:
- Tipo: **Scheduled**
- Nombre: `shopstream-daily-schedule`
- Cron expression: `cron(0 2 * * ? *)` (2:00 AM UTC todos los días)
- Acción: ejecutar el job ETL principal (el de Glue Studio)

### 3.3 Trigger Condicional (ETL → carga RDS)

En el workflow → Add trigger:
- Tipo: **Conditional**
- Nombre: `shopstream-conditional-load`
- Condición: el job ETL anterior terminó con estado **SUCCEEDED**
- Acción si SUCCEEDED: ejecutar job de carga final a RDS
- Si FAILED: enviar alerta por SNS

**Crear alerta SNS:**
```bash
# Crear topic SNS para alertas
aws sns create-topic --name shopstream-alerts --region us-east-1
aws sns subscribe --topic-arn <ARN> --protocol email --notification-endpoint alan.osorio@blend360.com
```

### 3.4 Documentar triggers en código

Crear `punto4_glue/workflow_triggers.json`:
```json
{
  "workflow_name": "shopstream-daily-workflow",
  "triggers": [
    {
      "name": "shopstream-daily-schedule",
      "type": "SCHEDULED",
      "schedule": "cron(0 2 * * ? *)",
      "description": "Ejecuta el workflow completo diariamente a 2:00 AM UTC",
      "actions": ["glue-etl-s3-to-processed"]
    },
    {
      "name": "shopstream-conditional-load",
      "type": "CONDITIONAL",
      "conditions": [
        {
          "job_name": "glue-etl-s3-to-processed",
          "state": "SUCCEEDED"
        }
      ],
      "description": "Solo carga a RDS si el ETL anterior fue exitoso",
      "actions": ["glue-load-rds"]
    }
  ]
}
```

---

## DÍA 4 — API REST con Flask + Zappa

### 4.1 Estructura del proyecto API

```
api/
├── app.py
├── db.py
├── handlers/
│   ├── __init__.py
│   ├── pages.py
│   ├── sessions.py
│   └── anomalies.py
├── zappa_settings.json
├── requirements.txt
└── tests/
    ├── test_pages.py
    ├── test_sessions.py
    └── test_anomalies.py
```

### 4.2 Dependencias

```bash
pip install flask zappa psycopg2-binary python-dotenv
pip freeze > api/requirements.txt
```

### 4.3 Conexión a RDS

Crear `api/db.py`:

```python
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return psycopg2.connect(
        host=os.environ["RDS_HOST"],
        user=os.environ["RDS_USER"],
        password=os.environ["RDS_PASSWORD"],
        dbname=os.environ.get("RDS_DB", "postgres"),
        cursor_factory=RealDictCursor
    )


def query(sql: str, params: tuple = None) -> list:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()
```

Crear `api/.env` (NO subir a GitHub):
```
RDS_HOST=<RDS_ENDPOINT>
RDS_USER=shopstream_admin
RDS_PASSWORD=<PASSWORD>
RDS_DB=postgres
```

Agregar `.env` al `.gitignore`.

### 4.4 Endpoint 1: GET /pages/top

Crear `api/handlers/pages.py`:

```python
from flask import request, jsonify
from db import query


def get_top_pages():
    metric = request.args.get("metric", "time_on_page")
    date = request.args.get("date")
    limit = int(request.args.get("limit", 10))

    if not date:
        return jsonify({"error": "Parámetro 'date' requerido (YYYY-MM-DD)"}), 400

    if metric == "time_on_page":
        order_col = "avg_time_seconds"
    elif metric == "bounce_rate":
        order_col = "bounce_rate"
    else:
        return jsonify({"error": "metric debe ser 'time_on_page' o 'bounce_rate'"}), 400

    sql = f"""
        SELECT page_url, page_type, avg_time_seconds, total_views, bounce_rate, date
        FROM shopstream.fact_page_metrics
        WHERE date = %s
        ORDER BY {order_col} DESC
        LIMIT %s
    """
    rows = query(sql, (date, limit))

    return jsonify({
        "metric": metric,
        "date": date,
        "limit": limit,
        "count": len(rows),
        "data": rows
    })
```

### 4.5 Endpoint 2: GET /sessions/summary

Crear `api/handlers/sessions.py`:

```python
from flask import request, jsonify
from db import query


def get_sessions_summary():
    country = request.args.get("country")
    device = request.args.get("device")
    date = request.args.get("date")

    if not date:
        return jsonify({"error": "Parámetro 'date' requerido (YYYY-MM-DD)"}), 400

    filters = ["date = %s"]
    params = [date]

    if country:
        filters.append("country = %s")
        params.append(country.upper())

    if device:
        filters.append("device_type = %s")
        params.append(device.lower())

    where_clause = " AND ".join(filters)

    sql = f"""
        SELECT device_type, country, avg_time_seconds, total_views, stddev_time, date
        FROM shopstream.fact_session_summary
        WHERE {where_clause}
        ORDER BY total_views DESC
    """
    rows = query(sql, tuple(params))

    return jsonify({
        "date": date,
        "country": country,
        "device": device,
        "count": len(rows),
        "data": rows
    })
```

### 4.6 Endpoint 3: GET /anomalies

Crear `api/handlers/anomalies.py`:

```python
from flask import request, jsonify
from db import query


def get_anomalies():
    date = request.args.get("date")

    if not date:
        return jsonify({"error": "Parámetro 'date' requerido (YYYY-MM-DD)"}), 400

    sql = """
        SELECT session_id, user_id, total_time, event_count,
               zscore_time, anomaly_type, date
        FROM shopstream.fact_anomalies
        WHERE date = %s
        ORDER BY zscore_time DESC
    """
    rows = query(sql, (date,))

    return jsonify({
        "date": date,
        "total_anomalies": len(rows),
        "data": rows
    })
```

### 4.7 App Principal Flask

Crear `api/app.py`:

```python
from flask import Flask
from handlers.pages import get_top_pages
from handlers.sessions import get_sessions_summary
from handlers.anomalies import get_anomalies

app = Flask(__name__)


@app.route("/pages/top", methods=["GET"])
def pages_top():
    return get_top_pages()


@app.route("/sessions/summary", methods=["GET"])
def sessions_summary():
    return get_sessions_summary()


@app.route("/anomalies", methods=["GET"])
def anomalies():
    return get_anomalies()


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "service": "ShopStream API"}, 200


if __name__ == "__main__":
    app.run(debug=True, port=5000)
```

### 4.8 Configurar Zappa para despliegue en Lambda

Crear `api/zappa_settings.json`:

```json
{
  "production": {
    "app_function": "app.app",
    "aws_region": "us-east-1",
    "profile_name": "default",
    "project_name": "shopstream-api",
    "runtime": "python3.11",
    "s3_bucket": "shopstream-raw-aad",
    "role_name": "LabRole",
    "environment_variables": {
      "RDS_HOST": "<RDS_ENDPOINT>",
      "RDS_USER": "shopstream_admin",
      "RDS_PASSWORD": "<PASSWORD>",
      "RDS_DB": "postgres"
    },
    "timeout_seconds": 30,
    "memory_size": 256
  }
}
```

**Desplegar la API:**
```bash
cd api/
zappa deploy production
# La URL del API Gateway aparecerá en la salida:
# https://xxxxxxxxx.execute-api.us-east-1.amazonaws.com/production

# Actualizar después de cambios:
zappa update production

# Probar los endpoints:
BASE_URL="https://xxxxxxxxx.execute-api.us-east-1.amazonaws.com/production"
curl "$BASE_URL/pages/top?metric=time_on_page&date=2025-01-15&limit=5"
curl "$BASE_URL/sessions/summary?country=CO&device=mobile&date=2025-01-15"
curl "$BASE_URL/anomalies?date=2025-01-15"
```

---

## DÍA 5-6 — Tests Unitarios API

Instalar dependencias de test:
```bash
pip install pytest pytest-cov moto boto3
```

Crear `api/tests/test_pages.py`:

```python
import pytest
import sys
import os
sys.path.insert(0, "api")

# Mock de la función query para tests sin RDS real
import unittest.mock as mock


def test_pages_top_missing_date():
    from app import app
    with app.test_client() as client:
        resp = client.get("/pages/top?metric=time_on_page")
        assert resp.status_code == 400
        data = resp.get_json()
        assert "error" in data


def test_pages_top_invalid_metric():
    from app import app
    with app.test_client() as client:
        resp = client.get("/pages/top?metric=INVALID&date=2025-01-15")
        assert resp.status_code == 400


def test_pages_top_valid_request():
    from app import app
    mock_rows = [
        {"page_url": "/home", "page_type": "home", "avg_time_seconds": 120.5,
         "total_views": 1000, "bounce_rate": 45.2, "date": "2025-01-15"}
    ]
    with app.test_client() as client:
        with mock.patch("handlers.pages.query", return_value=mock_rows):
            resp = client.get("/pages/top?metric=time_on_page&date=2025-01-15&limit=5")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["count"] == 1
            assert data["metric"] == "time_on_page"
            assert len(data["data"]) == 1
```

Crear `api/tests/test_sessions.py`:

```python
import sys
sys.path.insert(0, "api")
import unittest.mock as mock


def test_sessions_missing_date():
    from app import app
    with app.test_client() as client:
        resp = client.get("/sessions/summary")
        assert resp.status_code == 400


def test_sessions_with_filters():
    from app import app
    mock_rows = [
        {"device_type": "mobile", "country": "CO", "avg_time_seconds": 95.3,
         "total_views": 500, "stddev_time": 30.1, "date": "2025-01-15"}
    ]
    with app.test_client() as client:
        with mock.patch("handlers.sessions.query", return_value=mock_rows):
            resp = client.get("/sessions/summary?country=CO&device=mobile&date=2025-01-15")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["country"] == "CO"
            assert data["device"] == "mobile"
            assert data["count"] == 1
```

Crear `api/tests/test_anomalies.py`:

```python
import sys
sys.path.insert(0, "api")
import unittest.mock as mock


def test_anomalies_missing_date():
    from app import app
    with app.test_client() as client:
        resp = client.get("/anomalies")
        assert resp.status_code == 400


def test_anomalies_returns_list():
    from app import app
    mock_rows = [
        {"session_id": "s1", "user_id": "u1", "total_time": 9999,
         "event_count": 500, "zscore_time": 4.2, "anomaly_type": "zscore_time",
         "date": "2025-01-15"}
    ]
    with app.test_client() as client:
        with mock.patch("handlers.anomalies.query", return_value=mock_rows):
            resp = client.get("/anomalies?date=2025-01-15")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["total_anomalies"] == 1
            assert data["data"][0]["zscore_time"] == 4.2


def test_health_check():
    from app import app
    with app.test_client() as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "ok"
```

**Ejecutar tests:**
```bash
cd api/
pytest tests/ -v --cov=. --cov-report=term-missing
```

---

## Checklist Final Ana

- [ ] RDS PostgreSQL `shopstream-rds-dw` creado y accesible
- [ ] Schema DW aplicado: 3 tablas dim + 6 tablas fact
- [ ] Glue Crawler detecta Parquets de Daniela
- [ ] Glue Studio ETL visual con nodos gráficos (captura de pantalla obligatoria)
- [ ] Nodo Data Quality con ≥5 reglas configurado en Glue Studio
- [ ] Workflow `shopstream-daily-workflow` creado
- [ ] Trigger scheduled: `cron(0 2 * * ? *)` (2 AM UTC)
- [ ] Trigger condicional: carga a RDS solo si ETL = SUCCEEDED
- [ ] Alerta SNS si ETL falla
- [ ] 3 endpoints funcionales y respondiendo correctamente
- [ ] API desplegada con Zappa en Lambda + API Gateway
- [ ] `pytest` con ≥10 tests verdes
- [ ] URLs de la API documentadas en README
- [ ] Screenshots de Glue Studio en `docs/screenshots/`

---

## Comandos Útiles

```bash
# Probar conexión a RDS
psql -h <RDS_ENDPOINT> -U shopstream_admin -d postgres -c "\dt shopstream.*"

# Correr API localmente
cd api/ && python app.py
# Probar local:
curl "http://localhost:5000/pages/top?metric=time_on_page&date=2025-01-15&limit=3"
curl "http://localhost:5000/sessions/summary?date=2025-01-15"
curl "http://localhost:5000/anomalies?date=2025-01-15"

# Desplegar con Zappa
cd api/
zappa deploy production

# Ver logs de Lambda de la API
zappa tail production

# Actualizar código
zappa update production

# Correr tests
pytest api/tests/ -v

# Ver tablas en RDS
psql -h <RDS_ENDPOINT> -U shopstream_admin -d postgres \
  -c "SELECT COUNT(*) FROM shopstream.fact_page_metrics;"
```

---

## Notas Importantes

- **Glue Studio visual:** El job ETL **debe** crearse con el editor gráfico de nodos. No puede ser código puro. Tomar capturas de pantalla del grafo de nodos para la entrega.
- **Password de RDS:** guardar en `.env` que va en `.gitignore`. Nunca subir credenciales a GitHub.
- **Zappa y LabRole:** en la configuración de Zappa poner `"role_name": "LabRole"` porque Academy no permite crear roles nuevos.
- **Glue Connection:** necesitas crear una Glue Connection JDBC para que el nodo PostgreSQL Target del Studio pueda conectarse a RDS.
- **Data Quality rules:** documentar también en `punto4_glue/data_quality_rules.txt` las reglas configuradas en Glue Studio.
