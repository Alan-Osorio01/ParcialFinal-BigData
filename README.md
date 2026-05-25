# ShopStream — Pipeline Big Data en AWS

Parcial III · Big Data e Ingeniería de Datos · Semestre 8

Pipeline batch en AWS que procesa logs de comportamiento de usuarios de un e-commerce ficticio:
validación con Lambda, transformación distribuida con PySpark sobre EMR, ETL con Glue Studio
hacia un datawarehouse en RDS PostgreSQL, y exposición vía API REST con Lambda + Zappa.

## Integrantes

| Integrante | Responsabilidad | Plan detallado |
|---|---|---|
| Alan Osorio | Punto 1 (Datos) + Punto 2 (Ingesta) + CI/CD | [plan-trabajo/alan.md](plan-trabajo/alan.md) |
| Daniela | Punto 3 (PySpark + EMR) | [plan-trabajo/daniela.md](plan-trabajo/daniela.md) |
| Ana | Punto 4 (Glue + RDS) + API REST | [plan-trabajo/ana.md](plan-trabajo/ana.md) |

Visión general y cronograma: [plan-trabajo/division.md](plan-trabajo/division.md)

## Arquitectura

```
[Generador Python]
       │
       ▼
   S3 raw/year=YYYY/month=MM/day=DD/
       │ (PutObject trigger)
       ▼
   Lambda validación ──► quarantine/ si falla
       │
       ▼
   EMR Cluster + PySpark (6 métricas + anomalías)
       │
       ▼
   S3 processed/ (Parquet particionado)
       │
       ▼
   Glue Workflow (ETL Visual + Data Quality + Triggers)
       │
       ▼
   RDS PostgreSQL (Datawarehouse)
       │
       ▼
   API Gateway → 3 endpoints Lambda (Zappa)
```

## Estructura del Repositorio

```
ParcialFinal-BigData/
├── README.md
├── requirements.txt
├── .github/workflows/      # CI/CD GitHub Actions
├── plan-trabajo/           # Planes de trabajo por persona
├── infra/                  # Scripts de infraestructura (boto3)
├── scripts/                # Helpers y utilidades AWS
├── punto1_datos/           # Generación de datos sintéticos
├── punto2_ingesta/         # Lambda de validación
├── punto3_pyspark/         # Pipeline PySpark/EMR
├── punto4_glue/            # Glue Studio + DW schema
├── api/                    # API REST con Flask + Zappa
└── docs/                   # Documentación y screenshots
```

## Setup Inicial

1. Clonar el repo y configurar credenciales AWS Academy (en `.env`, no toca `~/.aws/`):
   ```bash
   git clone <repo>
   cd ParcialFinal-BigData
   cp .env.example .env          # solo la primera vez
   nano .env                     # pega aws_access_key_id, secret y session_token de Academy
   source scripts/load_aws_env.sh # cárgalas a tu shell actual
   ```
   > Las credenciales viven en `.env` (gitignoreado). Cuando expiren cada ~4 h,
   > solo editas `.env` y vuelves a sourcear el script.

2. Crear entorno virtual e instalar dependencias:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   pip install -r punto1_datos/requirements.txt
   ```

3. Crear infraestructura base (S3 buckets):
   ```bash
   python infra/01_create_s3_buckets.py
   ```

## Ejecución Rápida del Pipeline

```bash
# 1. Generar datos sintéticos del día
python punto1_datos/generator.py --date 2025-01-15 --events 500000

# 2. Subir a S3 (esto dispara la Lambda automáticamente)
python punto1_datos/upload_to_s3.py --date 2025-01-15

# 3. Verificar ingesta en CloudWatch
aws cloudwatch get-metric-statistics \
  --namespace ShopStream/Ingesta \
  --metric-name RegistrosValidos \
  --start-time 2025-01-15T00:00:00Z --end-time 2025-01-16T00:00:00Z \
  --period 3600 --statistics Sum
```

## Tests

```bash
pytest -v --cov=.
```

## Convenciones de Recursos AWS (cuenta Academy compartida)

| Recurso | Nombre |
|---|---|
| Región | `us-east-1` |
| IAM Role | `LabRole` (no crear roles nuevos) |
| S3 raw | `shopstream-raw-aad` |
| S3 processed | `shopstream-processed-aad` |
| S3 quarantine | `shopstream-quarantine-aad` |
| Lambda | `shopstream-ingesta-validator` |
| EMR | `shopstream-emr-cluster` |
| RDS | `shopstream-rds-dw` |
| Glue DB | `shopstream_glue_db` |
