# Alan — Fundación del Pipeline
## Punto 1 (Datos Sintéticos) + Punto 2 (Ingesta Lambda) + CI/CD

> **Empieza primero.** Todos dependen de tus datos en S3 y del repositorio configurado.
> Avisa al grupo cuando termines el Día 1 y cuando tengas datos subidos en el Día 2.

---

## Resumen de Responsabilidades

| # | Tarea | Entregable |
|---|---|---|
| 1 | Setup repo GitHub + ramas + estructura | Repo funcional con README |
| 2 | Crear recursos AWS (S3 buckets) | 3 buckets creados |
| 3 | Diseñar schemas de 5 entidades | `schemas/*.json` |
| 4 | Generador sintético ≥500k registros/día | `generator.py` |
| 5 | Subida particionada a S3 raw | Datos en `s3://shopstream-raw-aad/` |
| 6 | Lambda validación + quarantine | `lambda_function.py` desplegada |
| 7 | Métricas CloudWatch | Dashboard en CloudWatch |
| 8 | GitHub Actions CI/CD | `.github/workflows/ci.yml` |
| 9 | Tests unitarios propios | `pytest` verde |

---

## DÍA 1 — Setup de Infraestructura Base

### 1.1 Repositorio GitHub

```bash
# En el repo ya creado, configurar la estructura de ramas
git checkout -b dev
git checkout -b feature/alan-datos
git push origin dev feature/alan-datos

# Crear la estructura de carpetas
mkdir -p punto1_datos/schemas punto1_datos/tests
mkdir -p punto2_ingesta/tests
mkdir -p infra docs
```

### 1.2 Crear S3 Buckets (AWS Academy — us-east-1)

> AWS Academy usa `LabRole`. NO intentar crear roles IAM nuevos.

```bash
# Configurar AWS CLI con credenciales de Academy (copiar desde AWS Details)
aws configure
# Region: us-east-1
# Output: json

# Crear los 3 buckets
aws s3 mb s3://shopstream-raw-aad --region us-east-1
aws s3 mb s3://shopstream-processed-aad --region us-east-1
aws s3 mb s3://shopstream-quarantine-aad --region us-east-1

# Verificar
aws s3 ls | grep shopstream
```

Guardar en `infra/s3_setup.sh`:
```bash
#!/bin/bash
REGION="us-east-1"
aws s3 mb s3://shopstream-raw-aad --region $REGION
aws s3 mb s3://shopstream-processed-aad --region $REGION
aws s3 mb s3://shopstream-quarantine-aad --region $REGION
```

### 1.3 Diseñar Schemas (5 entidades)

Crear `punto1_datos/schemas/`:

**`users.json`**
```json
{
  "user_id": "string (UUID)",
  "name": "string",
  "email": "string",
  "country": "string (ISO 3166-1 alpha-2)",
  "created_at": "ISO 8601 datetime",
  "device_preference": "string (mobile|desktop|tablet)"
}
```

**`products.json`**
```json
{
  "product_id": "string (UUID)",
  "name": "string",
  "category": "string",
  "price": "float (> 0)",
  "stock": "integer (>= 0)",
  "created_at": "ISO 8601 datetime"
}
```

**`sessions.json`**
```json
{
  "session_id": "string (UUID)",
  "user_id": "string (UUID)",
  "started_at": "ISO 8601 datetime",
  "ended_at": "ISO 8601 datetime",
  "device_type": "string (mobile|desktop|tablet)",
  "country": "string"
}
```

**`events.json`** (superclase con 5 subtipos)
```json
{
  "event_id": "string (UUID)",
  "event_type": "string (page_view|click|search|product_view|cart_event)",
  "user_id": "string (UUID)",
  "session_id": "string (UUID)",
  "timestamp": "ISO 8601 datetime",
  "extra": "object (campos específicos por tipo)"
}
```

**`transactions.json`**
```json
{
  "tx_id": "string (UUID)",
  "user_id": "string (UUID)",
  "session_id": "string (UUID)",
  "product_id": "string (UUID)",
  "amount": "float",
  "status": "string (completed|failed|pending)",
  "created_at": "ISO 8601 datetime"
}
```

---

## DÍA 2 — Generador de Datos Sintéticos

### 2.1 Dependencias

```bash
pip install faker numpy pandas boto3 tqdm
pip freeze > punto1_datos/requirements.txt
```

### 2.2 Script Generador

Crear `punto1_datos/generator.py`:

```python
import uuid
import json
import random
import os
from datetime import datetime, timedelta
import numpy as np
from faker import Faker
import pandas as pd
from tqdm import tqdm

fake = Faker()
random.seed(42)
np.random.seed(42)

# Configuración
NUM_USERS = 5_000
NUM_PRODUCTS = 500
TARGET_DATE = datetime(2025, 1, 15)  # cambiar para generar distintos días
EVENTS_PER_DAY = 500_000

CATEGORIES = ["electronics", "clothing", "home", "sports", "beauty", "books", "toys"]
COUNTRIES = ["CO", "MX", "AR", "PE", "CL", "US", "BR", "EC"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
PAGE_TYPES = ["home", "category", "product", "cart", "checkout", "search"]
ELEMENT_TYPES = ["button", "link", "image", "banner", "product_card"]


def generate_users(n=NUM_USERS):
    users = []
    for _ in range(n):
        users.append({
            "user_id": str(uuid.uuid4()),
            "name": fake.name(),
            "email": fake.email(),
            "country": random.choice(COUNTRIES),
            "created_at": fake.date_time_between(start_date="-2y").isoformat(),
            "device_preference": random.choice(DEVICE_TYPES)
        })
    return users


def generate_products(n=NUM_PRODUCTS):
    products = []
    for _ in range(n):
        products.append({
            "product_id": str(uuid.uuid4()),
            "name": fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "price": round(np.random.lognormal(3.5, 1.0), 2),
            "stock": random.randint(0, 1000),
            "created_at": fake.date_time_between(start_date="-1y").isoformat()
        })
    return products


def generate_events(users, products, target_date, n=EVENTS_PER_DAY):
    events = []
    user_ids = [u["user_id"] for u in users]
    product_ids = [p["product_id"] for p in products]
    product_categories = {p["product_id"]: p["category"] for p in products}
    product_prices = {p["product_id"]: p["price"] for p in products}

    # Distribución realista de tipos de evento
    event_weights = {
        "page_view": 0.40,
        "click": 0.25,
        "search": 0.10,
        "product_view": 0.15,
        "cart_event": 0.10,
    }
    event_types = list(event_weights.keys())
    weights = list(event_weights.values())

    for _ in tqdm(range(n), desc="Generando eventos"):
        user_id = random.choice(user_ids)
        session_id = str(uuid.uuid4())
        event_type = random.choices(event_types, weights=weights, k=1)[0]
        country = random.choice(COUNTRIES)
        device_type = random.choice(DEVICE_TYPES)

        ts_offset = random.randint(0, 86399)
        timestamp = (target_date + timedelta(seconds=ts_offset)).isoformat()

        base = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "user_id": user_id,
            "session_id": session_id,
            "timestamp": timestamp,
        }

        if event_type == "page_view":
            base.update({
                "page_url": f"/{random.choice(PAGE_TYPES)}/{fake.slug()}",
                "page_type": random.choice(PAGE_TYPES),
                "time_on_page_seconds": int(np.random.exponential(120)),
                "referrer": random.choice(["google.com", "facebook.com", "direct", "instagram.com", ""]),
                "device_type": device_type,
                "country": country,
            })
        elif event_type == "click":
            base.update({
                "element_id": f"elem_{random.randint(1,500)}",
                "element_type": random.choice(ELEMENT_TYPES),
                "page_url": f"/{random.choice(PAGE_TYPES)}/{fake.slug()}",
                "x_position": random.randint(0, 1920),
                "y_position": random.randint(0, 1080),
            })
        elif event_type == "search":
            base.update({
                "query": fake.words(nb=random.randint(1, 4), unique=True),
                "results_count": random.randint(0, 200),
            })
        elif event_type == "product_view":
            pid = random.choice(product_ids)
            base.update({
                "product_id": pid,
                "category": product_categories[pid],
                "price": product_prices[pid],
                "time_on_page_seconds": int(np.random.exponential(90)),
            })
        elif event_type == "cart_event":
            base.update({
                "product_id": random.choice(product_ids),
                "action": random.choices(["add", "remove"], weights=[0.75, 0.25])[0],
            })

        events.append(base)

    return events


def generate_transactions(users, products, target_date, n=5000):
    transactions = []
    user_ids = [u["user_id"] for u in users]
    product_ids = [p["product_id"] for p in products]
    product_prices = {p["product_id"]: p["price"] for p in products}
    for _ in range(n):
        pid = random.choice(product_ids)
        transactions.append({
            "tx_id": str(uuid.uuid4()),
            "user_id": random.choice(user_ids),
            "session_id": str(uuid.uuid4()),
            "product_id": pid,
            "amount": product_prices[pid],
            "status": random.choices(["completed", "failed", "pending"], weights=[0.7, 0.1, 0.2])[0],
            "created_at": (target_date + timedelta(seconds=random.randint(0, 86399))).isoformat()
        })
    return transactions


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2025-01-15", help="YYYY-MM-DD")
    parser.add_argument("--output", default="./data_output", help="carpeta de salida")
    parser.add_argument("--events", type=int, default=500_000)
    args = parser.parse_args()

    target = datetime.strptime(args.date, "%Y-%m-%d")
    os.makedirs(args.output, exist_ok=True)

    print("Generando usuarios...")
    users = generate_users()

    print("Generando productos...")
    products = generate_products()

    print(f"Generando {args.events} eventos para {args.date}...")
    events = generate_events(users, products, target, args.events)

    print("Generando transacciones...")
    transactions = generate_transactions(users, products, target)

    # Guardar en JSON lines
    for name, data in [("users", users), ("products", products),
                       ("events", events), ("transactions", transactions)]:
        path = os.path.join(args.output, f"{name}.jsonl")
        with open(path, "w") as f:
            for row in data:
                f.write(json.dumps(row) + "\n")
        print(f"  Guardado {name}: {len(data):,} registros → {path}")

    print("Done.")
```

### 2.3 Script de Subida a S3 Particionado

Crear `punto1_datos/upload_to_s3.py`:

```python
import boto3
import os
import argparse
from datetime import datetime

def upload_partitioned(local_dir, bucket, date_str):
    s3 = boto3.client("s3", region_name="us-east-1")
    date = datetime.strptime(date_str, "%Y-%m-%d")
    prefix = f"year={date.year}/month={date.month:02d}/day={date.day:02d}"

    for filename in os.listdir(local_dir):
        if not filename.endswith(".jsonl"):
            continue
        local_path = os.path.join(local_dir, filename)
        s3_key = f"{prefix}/{filename}"
        print(f"Subiendo {local_path} → s3://{bucket}/{s3_key}")
        s3.upload_file(local_path, bucket, s3_key)

    print("Upload completo.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2025-01-15")
    parser.add_argument("--local-dir", default="./data_output")
    parser.add_argument("--bucket", default="shopstream-raw-aad")
    args = parser.parse_args()
    upload_partitioned(args.local_dir, args.bucket, args.date)
```

**Ejecutar todo:**
```bash
python punto1_datos/generator.py --date 2025-01-15 --events 500000
python punto1_datos/upload_to_s3.py --date 2025-01-15
# Verificar en S3
aws s3 ls s3://shopstream-raw-aad/year=2025/month=01/day=15/
```

---

## DÍA 3 — Lambda de Validación (Punto 2)

### 3.1 Arquitectura de la Lambda

La Lambda se activa cuando se sube un archivo a `s3://shopstream-raw-aad/`. Valida el esquema de cada línea del JSONL, mueve archivos inválidos a quarantine con metadata del error, y registra métricas en CloudWatch.

### 3.2 Código Lambda

Crear `punto2_ingesta/validators.py`:

```python
from datetime import datetime

REQUIRED_FIELDS = {
    "page_view": ["event_id", "user_id", "session_id", "timestamp", "page_url",
                  "page_type", "time_on_page_seconds", "device_type", "country"],
    "click": ["event_id", "user_id", "session_id", "timestamp", "element_id",
              "element_type", "page_url", "x_position", "y_position"],
    "search": ["event_id", "user_id", "session_id", "timestamp", "query", "results_count"],
    "product_view": ["event_id", "user_id", "session_id", "timestamp", "product_id",
                     "category", "price", "time_on_page_seconds"],
    "cart_event": ["event_id", "user_id", "session_id", "timestamp", "product_id", "action"],
}

def validate_event(event: dict) -> list[str]:
    errors = []
    event_type = event.get("event_type")
    if event_type not in REQUIRED_FIELDS:
        return [f"event_type inválido: {event_type}"]

    for field in REQUIRED_FIELDS[event_type]:
        if field not in event or event[field] is None:
            errors.append(f"Campo requerido faltante: {field}")

    # Validar timestamp
    ts = event.get("timestamp", "")
    try:
        datetime.fromisoformat(str(ts))
    except (ValueError, TypeError):
        errors.append(f"Timestamp inválido: {ts}")

    # Validar rangos numéricos
    if event_type == "page_view":
        tsp = event.get("time_on_page_seconds", -1)
        if not isinstance(tsp, (int, float)) or tsp < 0:
            errors.append(f"time_on_page_seconds inválido: {tsp}")

    if event_type == "product_view":
        price = event.get("price", -1)
        if not isinstance(price, (int, float)) or price <= 0:
            errors.append(f"price inválido: {price}")

    if event_type == "cart_event":
        action = event.get("action")
        if action not in ("add", "remove"):
            errors.append(f"action inválido: {action}")

    return errors
```

Crear `punto2_ingesta/lambda_function.py`:

```python
import json
import boto3
import os
import urllib.parse
from datetime import datetime
from validators import validate_event

s3 = boto3.client("s3")
cw = boto3.client("cloudwatch", region_name="us-east-1")

QUARANTINE_BUCKET = os.environ.get("QUARANTINE_BUCKET", "shopstream-quarantine-aad")
NAMESPACE = "ShopStream/Ingesta"


def put_metric(name, value, unit="Count"):
    cw.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[{
            "MetricName": name,
            "Value": value,
            "Unit": unit,
            "Dimensions": [{"Name": "Pipeline", "Value": "ShopStream"}]
        }]
    )


def lambda_handler(event, context):
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        size = record["s3"]["object"]["size"]

        print(f"Procesando: s3://{bucket}/{key} ({size} bytes)")
        put_metric("ArchivosRecibidos", 1)
        put_metric("TamanoArchivo", size, "Bytes")

        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            lines = [l for l in content.strip().split("\n") if l]
        except Exception as e:
            print(f"Error leyendo archivo: {e}")
            put_metric("ErroresLectura", 1)
            return {"statusCode": 500, "body": str(e)}

        errors_found = []
        valid_count = 0

        for i, line in enumerate(lines):
            try:
                evt = json.loads(line)
                errs = validate_event(evt)
                if errs:
                    errors_found.append({"line": i + 1, "errors": errs, "raw": line[:200]})
                else:
                    valid_count += 1
            except json.JSONDecodeError as e:
                errors_found.append({"line": i + 1, "errors": [f"JSON inválido: {e}"], "raw": line[:200]})

        put_metric("RegistrosValidos", valid_count)
        put_metric("RegistrosInvalidos", len(errors_found))

        if errors_found:
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            quarantine_key = f"quarantine/{ts}_{key.replace('/', '_')}"
            metadata_key = f"quarantine/{ts}_{key.replace('/', '_')}.metadata.json"

            # Copiar archivo original a quarantine
            s3.copy_object(
                Bucket=QUARANTINE_BUCKET,
                CopySource={"Bucket": bucket, "Key": key},
                Key=quarantine_key
            )

            # Guardar metadata del error
            metadata = {
                "original_key": key,
                "original_bucket": bucket,
                "processed_at": datetime.utcnow().isoformat(),
                "total_lines": len(lines),
                "valid_records": valid_count,
                "invalid_records": len(errors_found),
                "errors": errors_found[:50]  # primeros 50 errores
            }
            s3.put_object(
                Bucket=QUARANTINE_BUCKET,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType="application/json"
            )

            put_metric("ArchivosEnCuarentena", 1)
            print(f"Archivo movido a quarantine: {quarantine_key}")
        else:
            put_metric("ArchivosValidos", 1)
            print(f"Archivo válido: {valid_count} registros OK")

    return {"statusCode": 200, "body": "OK"}
```

### 3.3 Desplegar Lambda desde consola AWS Academy

1. Ir a **AWS Lambda** → **Create function** → Author from scratch
2. Nombre: `shopstream-ingesta-validator`
3. Runtime: Python 3.11
4. Execution role: **Use existing role → LabRole**
5. Subir el código (zip de `punto2_ingesta/`)
6. Variables de entorno: `QUARANTINE_BUCKET = shopstream-quarantine-aad`
7. **Add trigger** → S3 → bucket: `shopstream-raw-aad` → Event type: PUT

> Guardar el ARN de la Lambda en `infra/lambda_arn.txt`

---

## DÍA 4 — CloudWatch Dashboard

En la consola de CloudWatch → Dashboards → Create dashboard → `ShopStream-Ingesta`:

Widgets a crear:
- **Line chart:** `ArchivosRecibidos` en el tiempo
- **Number:** `RegistrosValidos` vs `RegistrosInvalidos` (hoy)
- **Bar chart:** `TamanoArchivo`
- **Alarm:** si `ErroresLectura > 5` en 5 minutos → SNS email

Documentar con screenshots en `docs/screenshots/cloudwatch_dashboard.png`.

---

## DÍA 4 — CI/CD GitHub Actions

Crear `.github/workflows/ci.yml`:

```yaml
name: ShopStream CI

on:
  push:
    branches: [main, dev]
  pull_request:
    branches: [main]

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install pytest pytest-cov flake8
          pip install -r punto1_datos/requirements.txt
          pip install -r punto2_ingesta/requirements.txt

      - name: Lint
        run: flake8 punto1_datos/ punto2_ingesta/ --max-line-length=120

      - name: Tests Punto 1
        run: pytest punto1_datos/tests/ -v --tb=short

      - name: Tests Punto 2
        run: pytest punto2_ingesta/tests/ -v --tb=short

      - name: Tests PySpark
        run: pytest punto3_pyspark/tests/ -v --tb=short
        continue-on-error: true  # Daniela maneja sus propios tests

      - name: Tests API
        run: pytest api/tests/ -v --tb=short
        continue-on-error: true  # Ana maneja sus propios tests
```

---

## DÍA 5 — Tests Unitarios

Crear `punto1_datos/tests/test_generator.py`:

```python
import pytest
import sys
sys.path.insert(0, "punto1_datos")
from generator import generate_users, generate_products, generate_events
from datetime import datetime

def test_users_count():
    users = generate_users(10)
    assert len(users) == 10

def test_user_fields():
    users = generate_users(1)
    u = users[0]
    assert "user_id" in u
    assert "email" in u
    assert u["device_preference"] in ("mobile", "desktop", "tablet")

def test_products_price_positive():
    products = generate_products(20)
    assert all(p["price"] > 0 for p in products)

def test_events_types():
    users = generate_users(5)
    products = generate_products(10)
    events = generate_events(users, products, datetime(2025, 1, 15), n=100)
    types = {e["event_type"] for e in events}
    assert types.issubset({"page_view", "click", "search", "product_view", "cart_event"})

def test_events_have_required_base_fields():
    users = generate_users(5)
    products = generate_products(10)
    events = generate_events(users, products, datetime(2025, 1, 15), n=50)
    for e in events:
        assert "event_id" in e
        assert "user_id" in e
        assert "session_id" in e
        assert "timestamp" in e
```

Crear `punto2_ingesta/tests/test_validators.py`:

```python
import pytest
import sys
sys.path.insert(0, "punto2_ingesta")
from validators import validate_event

def test_valid_page_view():
    evt = {
        "event_id": "abc",
        "event_type": "page_view",
        "user_id": "u1",
        "session_id": "s1",
        "timestamp": "2025-01-15T10:00:00",
        "page_url": "/home",
        "page_type": "home",
        "time_on_page_seconds": 30,
        "device_type": "mobile",
        "country": "CO",
    }
    assert validate_event(evt) == []

def test_invalid_event_type():
    evt = {"event_type": "unknown"}
    errs = validate_event(evt)
    assert any("inválido" in e for e in errs)

def test_missing_required_field():
    evt = {
        "event_id": "abc",
        "event_type": "page_view",
        "user_id": "u1",
        "session_id": "s1",
        # timestamp faltante
        "page_url": "/home",
        "page_type": "home",
        "time_on_page_seconds": 30,
        "device_type": "mobile",
        "country": "CO",
    }
    errs = validate_event(evt)
    assert any("timestamp" in e for e in errs)

def test_invalid_cart_action():
    evt = {
        "event_id": "abc",
        "event_type": "cart_event",
        "user_id": "u1",
        "session_id": "s1",
        "timestamp": "2025-01-15T10:00:00",
        "product_id": "p1",
        "action": "delete",  # inválido
    }
    errs = validate_event(evt)
    assert any("action" in e for e in errs)

def test_negative_price():
    evt = {
        "event_id": "abc",
        "event_type": "product_view",
        "user_id": "u1",
        "session_id": "s1",
        "timestamp": "2025-01-15T10:00:00",
        "product_id": "p1",
        "category": "electronics",
        "price": -10.0,
        "time_on_page_seconds": 45,
    }
    errs = validate_event(evt)
    assert any("price" in e for e in errs)
```

**Ejecutar tests:**
```bash
pytest punto1_datos/tests/ punto2_ingesta/tests/ -v --cov=punto1_datos --cov=punto2_ingesta
```

---

## Checklist Final Alan

- [ ] Repo GitHub creado con estructura completa
- [ ] 3 buckets S3 creados: raw, processed, quarantine
- [ ] `generator.py` genera ≥500k registros para cualquier fecha
- [ ] Datos subidos a `s3://shopstream-raw-aad/year=2025/month=01/day=15/`
- [ ] Lambda `shopstream-ingesta-validator` desplegada y con trigger S3
- [ ] Archivos inválidos se mueven a quarantine con metadata JSON
- [ ] CloudWatch dashboard con 4 widgets
- [ ] GitHub Actions CI corriendo en cada push
- [ ] `pytest` con ≥8 tests verdes
- [ ] **Avisar a Daniela y Ana** cuando S3 tenga datos

---

## Comandos Útiles

```bash
# Generar datos de prueba (rápido, 10k registros)
python punto1_datos/generator.py --date 2025-01-15 --events 10000

# Generar datos completos (500k)
python punto1_datos/generator.py --date 2025-01-15 --events 500000

# Subir a S3
python punto1_datos/upload_to_s3.py --date 2025-01-15

# Ver lo que está en S3
aws s3 ls s3://shopstream-raw-aad/ --recursive

# Ver quarantine
aws s3 ls s3://shopstream-quarantine-aad/ --recursive

# Correr tests
pytest punto1_datos/tests/ punto2_ingesta/tests/ -v
```
