"""
ShopStream Lambda — Validación y Triage de Ingesta
====================================================

Trigger:  S3 PutObject en bucket raw
Acción:   Lee cada línea JSONL, valida el schema, y:
  - Si TODAS las líneas son válidas → emite métricas de éxito
  - Si HAY líneas inválidas → copia el archivo a quarantine/ + metadata JSON
  - Siempre publica métricas en CloudWatch
"""
import json
import os
import urllib.parse
from datetime import datetime, timezone

import boto3

from validators import validate_event

s3 = boto3.client("s3")
cw = boto3.client("cloudwatch")

QUARANTINE_BUCKET = os.environ.get("QUARANTINE_BUCKET", "shopstream-quarantine-aad")
NAMESPACE = os.environ.get("CLOUDWATCH_NAMESPACE", "ShopStream/Ingesta")

# Solo validamos eventos (events.jsonl). Otros JSONL (users, products, etc.)
# pasan derecho — no son eventos del usuario.
VALIDATE_ONLY_FILENAMES = {"events.jsonl"}


def put_metrics(metrics: list[dict]) -> None:
    if not metrics:
        return
    # CloudWatch acepta máximo 1000 puntos por request, agrupamos por seguridad
    for i in range(0, len(metrics), 20):
        cw.put_metric_data(Namespace=NAMESPACE, MetricData=metrics[i:i + 20])


def build_metric(name: str, value: float, unit: str = "Count") -> dict:
    return {
        "MetricName": name,
        "Value": value,
        "Unit": unit,
        "Dimensions": [{"Name": "Pipeline", "Value": "ShopStream"}],
    }


def should_validate(key: str) -> bool:
    filename = key.rsplit("/", 1)[-1]
    return filename in VALIDATE_ONLY_FILENAMES


def quarantine_file(src_bucket: str, src_key: str, errors: list, total_lines: int, valid_count: int) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_key = src_key.replace("/", "_")
    q_key = f"quarantine/{ts}_{safe_key}"
    meta_key = f"{q_key}.metadata.json"

    s3.copy_object(
        Bucket=QUARANTINE_BUCKET,
        CopySource={"Bucket": src_bucket, "Key": src_key},
        Key=q_key,
    )

    metadata = {
        "original_bucket": src_bucket,
        "original_key": src_key,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "total_lines": total_lines,
        "valid_records": valid_count,
        "invalid_records": len(errors),
        "errors_sample": errors[:50],  # primeros 50 errores
    }
    s3.put_object(
        Bucket=QUARANTINE_BUCKET,
        Key=meta_key,
        Body=json.dumps(metadata, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    return q_key


def process_object(bucket: str, key: str, size: int) -> dict:
    metrics = [
        build_metric("ArchivosRecibidos", 1),
        build_metric("TamanoArchivo", size, "Bytes"),
    ]

    if not should_validate(key):
        # Archivos no-evento (users, products, etc.) pasan derecho
        metrics.append(build_metric("ArchivosOmitidos", 1))
        put_metrics(metrics)
        return {"status": "skipped", "key": key, "reason": "no es events.jsonl"}

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        lines = [l for l in content.splitlines() if l.strip()]
    except Exception as e:
        metrics.append(build_metric("ErroresLectura", 1))
        put_metrics(metrics)
        return {"status": "read_error", "key": key, "error": str(e)}

    errors = []
    valid_count = 0
    for i, line in enumerate(lines, start=1):
        try:
            evt = json.loads(line)
        except json.JSONDecodeError as e:
            errors.append({"line": i, "errors": [f"JSON inválido: {e}"], "raw": line[:200]})
            continue
        errs = validate_event(evt)
        if errs:
            errors.append({"line": i, "errors": errs, "raw": line[:200]})
        else:
            valid_count += 1

    metrics.append(build_metric("RegistrosValidos", valid_count))
    metrics.append(build_metric("RegistrosInvalidos", len(errors)))

    if errors:
        q_key = quarantine_file(bucket, key, errors, len(lines), valid_count)
        metrics.append(build_metric("ArchivosEnCuarentena", 1))
        put_metrics(metrics)
        return {
            "status": "quarantined",
            "key": key,
            "quarantine_key": q_key,
            "total_lines": len(lines),
            "valid": valid_count,
            "invalid": len(errors),
        }

    metrics.append(build_metric("ArchivosValidos", 1))
    put_metrics(metrics)
    return {
        "status": "ok",
        "key": key,
        "total_lines": len(lines),
        "valid": valid_count,
    }


def lambda_handler(event, context):
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        size = record["s3"]["object"].get("size", 0)

        print(f"Procesando: s3://{bucket}/{key} ({size} bytes)")
        result = process_object(bucket, key, size)
        print(f"Resultado: {json.dumps(result)}")
        results.append(result)

    return {"statusCode": 200, "body": json.dumps({"results": results})}
