#!/usr/bin/env python3
"""
ShopStream — Infra Paso 2: Desplegar Lambda de ingesta
========================================================

Empaqueta y despliega la Lambda de validación, y configura:
  - Trigger S3 PutObject en el bucket raw
  - Permisos para que S3 pueda invocar la Lambda
  - Variables de entorno (QUARANTINE_BUCKET, CLOUDWATCH_NAMESPACE)

Idempotente: si la Lambda ya existe, actualiza el código y la configuración.

Uso:
    python infra/02_deploy_lambda.py
    python infra/02_deploy_lambda.py --teardown
"""
import argparse
import io
import json
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    AWS_REGION,
    IAM_ROLE_NAME,
    LAMBDA_FUNCTION_NAME,
    LAMBDA_RUNTIME,
    LAMBDA_TIMEOUT_SECONDS,
    LAMBDA_MEMORY_MB,
    S3_BUCKET_RAW,
    S3_BUCKET_QUARANTINE,
    CLOUDWATCH_NAMESPACE,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
LAMBDA_SRC = REPO_ROOT / "punto2_ingesta"


def get_role_arn(role_name: str) -> str:
    """Obtiene el ARN del LabRole de AWS Academy."""
    iam = boto3.client("iam")
    try:
        resp = iam.get_role(RoleName=role_name)
        return resp["Role"]["Arn"]
    except ClientError as e:
        print(f"✗ No se encontró el rol {role_name}: {e}")
        print("  En AWS Academy debe existir 'LabRole'. Verifica con: aws iam list-roles")
        sys.exit(1)


def build_zip() -> bytes:
    """Empaqueta lambda_function.py + validators.py en un zip en memoria."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename in ("lambda_function.py", "validators.py"):
            zf.write(LAMBDA_SRC / filename, arcname=filename)
    buf.seek(0)
    data = buf.read()
    print(f"  ✓ Zip empaquetado: {len(data) / 1024:.1f} KB")
    return data


def lambda_exists(client, name: str) -> bool:
    try:
        client.get_function(FunctionName=name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


def create_lambda(client, role_arn: str, zip_bytes: bytes) -> str:
    print(f"  → Creando función Lambda {LAMBDA_FUNCTION_NAME}...")
    resp = client.create_function(
        FunctionName=LAMBDA_FUNCTION_NAME,
        Runtime=LAMBDA_RUNTIME,
        Role=role_arn,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": zip_bytes},
        Timeout=LAMBDA_TIMEOUT_SECONDS,
        MemorySize=LAMBDA_MEMORY_MB,
        Environment={
            "Variables": {
                "QUARANTINE_BUCKET": S3_BUCKET_QUARANTINE,
                "CLOUDWATCH_NAMESPACE": CLOUDWATCH_NAMESPACE,
            }
        },
        Tags={"Project": "ShopStream", "Course": "BigData-Parcial3"},
    )
    print(f"  ✓ Lambda creada: {resp['FunctionArn']}")
    return resp["FunctionArn"]


def update_lambda(client, zip_bytes: bytes) -> str:
    print(f"  → Actualizando código de Lambda {LAMBDA_FUNCTION_NAME}...")
    client.update_function_code(
        FunctionName=LAMBDA_FUNCTION_NAME, ZipFile=zip_bytes
    )
    # Esperar a que termine la actualización del código antes de cambiar config
    wait_until_updated(client)

    client.update_function_configuration(
        FunctionName=LAMBDA_FUNCTION_NAME,
        Timeout=LAMBDA_TIMEOUT_SECONDS,
        MemorySize=LAMBDA_MEMORY_MB,
        Environment={
            "Variables": {
                "QUARANTINE_BUCKET": S3_BUCKET_QUARANTINE,
                "CLOUDWATCH_NAMESPACE": CLOUDWATCH_NAMESPACE,
            }
        },
    )
    wait_until_updated(client)
    resp = client.get_function(FunctionName=LAMBDA_FUNCTION_NAME)
    arn = resp["Configuration"]["FunctionArn"]
    print(f"  ✓ Lambda actualizada: {arn}")
    return arn


def wait_until_updated(client, timeout: int = 60) -> None:
    for _ in range(timeout):
        resp = client.get_function(FunctionName=LAMBDA_FUNCTION_NAME)
        state = resp["Configuration"].get("LastUpdateStatus", "Successful")
        if state == "Successful":
            return
        if state == "Failed":
            raise RuntimeError(f"Lambda update failed: {resp['Configuration']}")
        time.sleep(1)


def add_s3_invoke_permission(lambda_client, lambda_arn: str, bucket: str) -> None:
    """Permite que S3 invoque la Lambda (idempotente)."""
    statement_id = f"s3invoke-{bucket}"
    account_id = lambda_arn.split(":")[4]
    try:
        lambda_client.add_permission(
            FunctionName=LAMBDA_FUNCTION_NAME,
            StatementId=statement_id,
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=f"arn:aws:s3:::{bucket}",
            SourceAccount=account_id,
        )
        print(f"  ✓ Permiso de invocación desde S3 agregado")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            print(f"  · Permiso de S3 ya existía (skip)")
        else:
            raise


def configure_s3_notification(s3_client, bucket: str, lambda_arn: str) -> None:
    """Crea la notificación S3 → Lambda en PutObject."""
    # Importante: usar s3:ObjectCreated:* para capturar TANTO
    #   - Put (archivos pequeños subidos en un solo request)
    #   - CompleteMultipartUpload (archivos > 8 MB que boto3 sube en partes)
    # Si solo se pone "Put", los archivos grandes (events.jsonl) NUNCA disparan la Lambda.
    config = {
        "LambdaFunctionConfigurations": [
            {
                "Id": "shopstream-ingesta-trigger",
                "LambdaFunctionArn": lambda_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [{"Name": "suffix", "Value": ".jsonl"}]
                    }
                },
            }
        ]
    }
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket, NotificationConfiguration=config
    )
    print(f"  ✓ Notificación S3 PutObject (*.jsonl) → Lambda configurada")


def deploy() -> None:
    print("=" * 60)
    print(" ShopStream — Despliegue de Lambda de Ingesta")
    print("=" * 60)

    print("\n→ Obteniendo ARN del LabRole...")
    role_arn = get_role_arn(IAM_ROLE_NAME)
    print(f"  ✓ Role ARN: {role_arn}")

    print("\n→ Empaquetando código...")
    zip_bytes = build_zip()

    lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    print("\n→ Desplegando Lambda...")
    if lambda_exists(lambda_client, LAMBDA_FUNCTION_NAME):
        lambda_arn = update_lambda(lambda_client, zip_bytes)
    else:
        lambda_arn = create_lambda(lambda_client, role_arn, zip_bytes)
        # Tiempo de gracia para que IAM propague
        time.sleep(5)

    print(f"\n→ Configurando trigger S3 → Lambda en {S3_BUCKET_RAW}...")
    add_s3_invoke_permission(lambda_client, lambda_arn, S3_BUCKET_RAW)
    configure_s3_notification(s3_client, S3_BUCKET_RAW, lambda_arn)

    # Guardar ARN para referencia
    arn_file = REPO_ROOT / "infra" / "lambda_arn.txt"
    arn_file.write_text(lambda_arn + "\n")

    print("\n" + "=" * 60)
    print(" Despliegue OK")
    print(f"   Función:  {LAMBDA_FUNCTION_NAME}")
    print(f"   ARN:      {lambda_arn}")
    print(f"   Trigger:  s3://{S3_BUCKET_RAW}/*.jsonl (PutObject)")
    print(f"   Logs:     aws logs tail /aws/lambda/{LAMBDA_FUNCTION_NAME} --follow")
    print("=" * 60)


def teardown() -> None:
    print("=" * 60)
    print(" Eliminando Lambda + trigger S3")
    print("=" * 60)
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    lambda_client = boto3.client("lambda", region_name=AWS_REGION)

    # Quitar notificación S3
    try:
        s3_client.put_bucket_notification_configuration(
            Bucket=S3_BUCKET_RAW, NotificationConfiguration={}
        )
        print(f"  ✓ Notificación S3 removida de {S3_BUCKET_RAW}")
    except ClientError as e:
        print(f"  · S3 notif skip: {e}")

    # Borrar Lambda
    try:
        lambda_client.delete_function(FunctionName=LAMBDA_FUNCTION_NAME)
        print(f"  ✓ Lambda {LAMBDA_FUNCTION_NAME} eliminada")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"  · Lambda no existía (skip)")
        else:
            raise

    print("=" * 60)


def verify_identity() -> None:
    sts = boto3.client("sts", region_name=AWS_REGION)
    try:
        ident = sts.get_caller_identity()
        print(f"Cuenta AWS: {ident['Account']}")
    except Exception as e:
        print(f"✗ Credenciales AWS inválidas: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--teardown", action="store_true")
    args = parser.parse_args()

    verify_identity()
    if args.teardown:
        teardown()
    else:
        deploy()


if __name__ == "__main__":
    main()
