"""
Configuración central del proyecto ShopStream.
Cambia aquí cualquier valor — todos los scripts de infra y módulos lo importan.
"""

# Región AWS (Academy obliga us-east-1)
AWS_REGION = "us-east-1"

# IAM Role disponible en AWS Academy (no se puede crear roles nuevos)
IAM_ROLE_NAME = "LabRole"

# Sufijo de los buckets (cámbialo si hay colisión con otra cuenta)
BUCKET_SUFFIX = "aad"

# Nombres de buckets S3
S3_BUCKET_RAW = f"shopstream-raw-{BUCKET_SUFFIX}"
S3_BUCKET_PROCESSED = f"shopstream-processed-{BUCKET_SUFFIX}"
S3_BUCKET_QUARANTINE = f"shopstream-quarantine-{BUCKET_SUFFIX}"

# Lambda
LAMBDA_FUNCTION_NAME = "shopstream-ingesta-validator"
LAMBDA_RUNTIME = "python3.11"
LAMBDA_TIMEOUT_SECONDS = 300
LAMBDA_MEMORY_MB = 1024

# CloudWatch
CLOUDWATCH_NAMESPACE = "ShopStream/Ingesta"
CLOUDWATCH_DASHBOARD = "ShopStream-Ingesta"

# Glue
GLUE_DATABASE = "shopstream_glue_db"
GLUE_WORKFLOW = "shopstream-daily-workflow"

# RDS
RDS_IDENTIFIER = "shopstream-rds-dw"

# EMR
EMR_CLUSTER_NAME = "shopstream-emr-cluster"


def all_buckets() -> list[str]:
    return [S3_BUCKET_RAW, S3_BUCKET_PROCESSED, S3_BUCKET_QUARANTINE]
