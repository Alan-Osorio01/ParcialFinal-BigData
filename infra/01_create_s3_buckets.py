#!/usr/bin/env python3
"""
ShopStream — Infra Paso 1: Crear buckets S3
============================================

Crea los 3 buckets necesarios para el pipeline:
  - shopstream-raw-aad        (datos crudos del generador)
  - shopstream-processed-aad  (Parquet de PySpark)
  - shopstream-quarantine-aad (archivos inválidos detectados por Lambda)

Características:
  - Idempotente: si el bucket ya existe en tu cuenta, no falla
  - Bloque público total (security best practice)
  - Versionado activado en raw (para auditoría)
  - Tags para facilitar limpieza posterior

Uso:
    python infra/01_create_s3_buckets.py
    python infra/01_create_s3_buckets.py --teardown   # eliminar los buckets
"""
import argparse
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Permitir importar config.py cuando se ejecuta directamente
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    AWS_REGION,
    S3_BUCKET_RAW,
    S3_BUCKET_PROCESSED,
    S3_BUCKET_QUARANTINE,
    all_buckets,
)


def create_bucket(s3, name: str, region: str) -> str:
    """Crea el bucket si no existe. Retorna 'created' | 'exists' | 'owned-by-other'."""
    try:
        # us-east-1 NO acepta LocationConstraint — manejo especial
        if region == "us-east-1":
            s3.create_bucket(Bucket=name)
        else:
            s3.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        return "created"
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "BucketAlreadyOwnedByYou":
            return "exists"
        if code == "BucketAlreadyExists":
            return "owned-by-other"
        raise


def block_public_access(s3, name: str) -> None:
    s3.put_public_access_block(
        Bucket=name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )


def enable_versioning(s3, name: str) -> None:
    s3.put_bucket_versioning(
        Bucket=name,
        VersioningConfiguration={"Status": "Enabled"},
    )


def tag_bucket(s3, name: str, role: str) -> None:
    s3.put_bucket_tagging(
        Bucket=name,
        Tagging={
            "TagSet": [
                {"Key": "Project", "Value": "ShopStream"},
                {"Key": "Course", "Value": "BigData-Parcial3"},
                {"Key": "Role", "Value": role},
            ]
        },
    )


def setup_bucket(s3, name: str, role: str, region: str, enable_version: bool) -> None:
    print(f"\n→ Bucket: {name}")
    status = create_bucket(s3, name, region)
    if status == "created":
        print("  ✓ Bucket creado")
    elif status == "exists":
        print("  · Bucket ya existe en tu cuenta (skip create)")
    else:
        print("  ✗ El nombre lo tiene otra cuenta. Cambia BUCKET_SUFFIX en infra/config.py")
        sys.exit(1)

    block_public_access(s3, name)
    print("  ✓ Public access block aplicado")

    tag_bucket(s3, name, role)
    print(f"  ✓ Tags aplicados (Role={role})")

    if enable_version:
        enable_versioning(s3, name)
        print("  ✓ Versionado activado")


def teardown_bucket(s3, name: str) -> None:
    print(f"\n→ Eliminando bucket: {name}")
    try:
        # Borrar todas las versiones y delete markers
        paginator = s3.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=name):
            objects = []
            for v in page.get("Versions", []):
                objects.append({"Key": v["Key"], "VersionId": v["VersionId"]})
            for m in page.get("DeleteMarkers", []):
                objects.append({"Key": m["Key"], "VersionId": m["VersionId"]})
            if objects:
                s3.delete_objects(Bucket=name, Delete={"Objects": objects})
                print(f"  · {len(objects)} versiones eliminadas")

        s3.delete_bucket(Bucket=name)
        print("  ✓ Bucket eliminado")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "NoSuchBucket":
            print("  · No existe (skip)")
        else:
            raise


def verify_identity() -> None:
    """Verifica que hay credenciales AWS válidas antes de hacer cualquier cosa."""
    sts = boto3.client("sts", region_name=AWS_REGION)
    try:
        ident = sts.get_caller_identity()
        print(f"Cuenta AWS: {ident['Account']}")
        print(f"User/Role:  {ident['Arn']}")
    except Exception as e:
        print(f"✗ No hay credenciales AWS válidas: {e}")
        print("  Ejecuta primero: bash scripts/aws_setup.sh")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--teardown",
        action="store_true",
        help="Eliminar los buckets (PELIGROSO — borra todo el contenido)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print(" ShopStream — Setup de S3 Buckets")
    print("=" * 60)
    verify_identity()
    print(f"Región: {AWS_REGION}\n")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    if args.teardown:
        confirm = input("¿Eliminar TODOS los buckets y su contenido? (escribe 'si'): ")
        if confirm.strip().lower() != "si":
            print("Cancelado.")
            return
        for b in all_buckets():
            teardown_bucket(s3, b)
        print("\n" + "=" * 60)
        print(" Teardown completo.")
        print("=" * 60)
        return

    # Versionado solo en raw (auditoría de archivos originales)
    setup_bucket(s3, S3_BUCKET_RAW, role="raw", region=AWS_REGION, enable_version=True)
    setup_bucket(s3, S3_BUCKET_PROCESSED, role="processed", region=AWS_REGION, enable_version=False)
    setup_bucket(s3, S3_BUCKET_QUARANTINE, role="quarantine", region=AWS_REGION, enable_version=False)

    print("\n" + "=" * 60)
    print(" Buckets listos. Verifica con:")
    print(f"   aws s3 ls | grep shopstream")
    print("=" * 60)


if __name__ == "__main__":
    main()
