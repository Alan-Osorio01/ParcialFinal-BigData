#!/usr/bin/env python3
"""
ShopStream — Subida particionada a S3
======================================

Sube los .jsonl generados a S3 con particiones year=YYYY/month=MM/day=DD.

Estructura resultante en S3:
    s3://shopstream-raw-aad/
        year=2025/month=01/day=15/
            events.jsonl
            users.jsonl
            products.jsonl
            sessions.jsonl
            transactions.jsonl

Uso:
    python punto1_datos/upload_to_s3.py --date 2025-01-15
    python punto1_datos/upload_to_s3.py --date 2025-01-15 --local-dir ./test_data
"""
import argparse
import os
import sys
import threading
from datetime import datetime
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from tqdm import tqdm

# Importar config central
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "infra"))
from config import AWS_REGION, S3_BUCKET_RAW

# Multipart para archivos > 8 MB, chunks de 8 MB, 4 threads en paralelo
TRANSFER_CFG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=8 * 1024 * 1024,
    max_concurrency=4,
    use_threads=True,
)


class _ProgressBar:
    """Callback para mostrar progreso de subida (thread-safe)."""

    def __init__(self, filename: str, total: int):
        self._lock = threading.Lock()
        self._bar = tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=filename,
            leave=True,
        )

    def __call__(self, bytes_amount: int) -> None:
        with self._lock:
            self._bar.update(bytes_amount)

    def close(self):
        self._bar.close()


def build_partition_prefix(date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return f"year={d.year}/month={d.month:02d}/day={d.day:02d}"


def upload_file(s3, local_path: str, bucket: str, key: str) -> int:
    size = os.path.getsize(local_path)
    filename = os.path.basename(local_path)
    progress = _ProgressBar(filename, size)
    try:
        s3.upload_file(local_path, bucket, key, Config=TRANSFER_CFG, Callback=progress)
    finally:
        progress.close()
    return size


def upload_partitioned(local_dir: str, bucket: str, date_str: str) -> None:
    if not os.path.isdir(local_dir):
        print(f"✗ Directorio local no existe: {local_dir}")
        sys.exit(1)

    files = sorted(f for f in os.listdir(local_dir) if f.endswith(".jsonl"))
    if not files:
        print(f"✗ No hay archivos .jsonl en {local_dir}")
        sys.exit(1)

    prefix = build_partition_prefix(date_str)
    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Total para mostrar antes de empezar
    sizes = {f: os.path.getsize(os.path.join(local_dir, f)) for f in files}
    total_input = sum(sizes.values())
    print(f"Subiendo {len(files)} archivos ({total_input / 1024 / 1024:.1f} MB) a s3://{bucket}/{prefix}/")
    print("Multipart automático para archivos > 8 MB, 4 threads en paralelo.\n")

    total_bytes = 0
    for filename in files:
        local_path = os.path.join(local_dir, filename)
        s3_key = f"{prefix}/{filename}"
        try:
            size = upload_file(s3, local_path, bucket, s3_key)
            total_bytes += size
        except ClientError as e:
            print(f"  ✗ {filename}: {e}")
            sys.exit(1)

    print(f"\nTotal subido: {total_bytes / 1024 / 1024:.1f} MB")
    print(f"Ver con:  aws s3 ls s3://{bucket}/{prefix}/")


def verify_identity() -> None:
    sts = boto3.client("sts", region_name=AWS_REGION)
    try:
        sts.get_caller_identity()
    except Exception as e:
        print(f"✗ Credenciales AWS inválidas: {e}")
        print("  Ejecuta primero: bash scripts/aws_setup.sh")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--local-dir", default="./data_output")
    parser.add_argument("--bucket", default=S3_BUCKET_RAW)
    args = parser.parse_args()

    verify_identity()
    upload_partitioned(args.local_dir, args.bucket, args.date)


if __name__ == "__main__":
    main()
