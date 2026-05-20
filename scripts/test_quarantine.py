#!/usr/bin/env python3
"""
Test de extremo a extremo del flujo de cuarentena.
========================================================
Genera un events.jsonl con líneas inválidas mezcladas con válidas,
lo sube a S3, y verifica que la Lambda lo movió a quarantine/ con metadata.

Uso:
    python scripts/test_quarantine.py
"""
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "infra"))
from config import AWS_REGION, S3_BUCKET_RAW, S3_BUCKET_QUARANTINE

# Fecha distinta a la de pruebas reales para no pisar datos válidos
TEST_DATE = "2099-12-31"
PARTITION = "year=2099/month=12/day=31"
TEST_KEY = f"{PARTITION}/events.jsonl"


def build_test_events() -> str:
    """Construye un .jsonl con 5 válidos y 5 inválidos (varios tipos de error)."""
    valid_template = {
        "event_id": "v",
        "event_type": "page_view",
        "user_id": "u1",
        "session_id": "s1",
        "timestamp": "2099-12-31T10:00:00",
        "page_url": "/home",
        "page_type": "home",
        "time_on_page_seconds": 30,
        "device_type": "mobile",
        "country": "CO",
    }

    lines = []

    # 5 válidos
    for i in range(5):
        evt = dict(valid_template, event_id=f"valid-{i}")
        lines.append(json.dumps(evt))

    # 1 inválido: event_type desconocido
    lines.append(json.dumps({**valid_template, "event_id": "bad-1", "event_type": "UNKNOWN"}))

    # 2 inválido: price negativo en product_view
    lines.append(json.dumps({
        "event_id": "bad-2", "event_type": "product_view",
        "user_id": "u1", "session_id": "s1",
        "timestamp": "2099-12-31T10:00:00",
        "product_id": "p1", "category": "electronics",
        "price": -50, "time_on_page_seconds": 10,
    }))

    # 3 inválido: timestamp roto
    lines.append(json.dumps({**valid_template, "event_id": "bad-3", "timestamp": "not-a-date"}))

    # 4 inválido: device_type no soportado
    lines.append(json.dumps({**valid_template, "event_id": "bad-4", "device_type": "smartwatch"}))

    # 5 JSON malformado (no es JSON parseable)
    lines.append("{esto no es json válido}")

    return "\n".join(lines)


def main():
    s3 = boto3.client("s3", region_name=AWS_REGION)

    print("=" * 60)
    print(" Test de cuarentena — flujo end-to-end")
    print("=" * 60)
    print(f"  Partición de test: {PARTITION}")
    print(f"  Buckets raw/quarantine: {S3_BUCKET_RAW} / {S3_BUCKET_QUARANTINE}")
    print()

    # 0. Limpiar quarantine previo de este test (para verificar limpiamente)
    print("→ Limpiando archivos previos de test en quarantine...")
    listing = s3.list_objects_v2(Bucket=S3_BUCKET_QUARANTINE, Prefix="quarantine/")
    prev_count = listing.get("KeyCount", 0)
    print(f"  Quarantine actual: {prev_count} objetos\n")

    # 1. Subir el archivo
    body = build_test_events()
    total_lines = len(body.split("\n"))
    expected_invalid = 5  # 4 con campos malos + 1 JSON malformado
    expected_valid = total_lines - expected_invalid

    print(f"→ Subiendo events.jsonl ({total_lines} líneas: "
          f"{expected_valid} válidas, {expected_invalid} inválidas)")
    s3.put_object(
        Bucket=S3_BUCKET_RAW,
        Key=TEST_KEY,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    print(f"  ✓ Subido a s3://{S3_BUCKET_RAW}/{TEST_KEY}\n")

    # 2. Esperar a que la Lambda procese (es rápido pero hay latencia de notificación)
    print("→ Esperando 12s a que la Lambda procese y mueva a quarantine...")
    time.sleep(12)

    # 3. Verificar quarantine
    print(f"\n→ Verificando contenido de s3://{S3_BUCKET_QUARANTINE}/quarantine/")
    listing = s3.list_objects_v2(Bucket=S3_BUCKET_QUARANTINE, Prefix="quarantine/")
    contents = listing.get("Contents", [])
    new_count = len(contents) - prev_count
    print(f"  Total ahora: {len(contents)} objetos (Δ +{new_count})\n")

    # Buscar el más reciente
    if not contents:
        print("✗ FALLO: no hay nada en quarantine. La Lambda no se disparó.")
        sys.exit(1)

    contents.sort(key=lambda o: o["LastModified"], reverse=True)
    latest = contents[:2]  # esperamos el .jsonl + el .metadata.json más recientes

    print("Últimos objetos en quarantine:")
    for obj in latest:
        size_kb = obj["Size"] / 1024
        print(f"  • {obj['Key']}  ({size_kb:.1f} KB, {obj['LastModified']})")

    # 4. Leer y mostrar el metadata
    metadata_obj = next((o for o in latest if o["Key"].endswith(".metadata.json")), None)
    if not metadata_obj:
        print("\n✗ FALLO: no se encontró metadata.json (esperado al lado del archivo)")
        sys.exit(1)

    print(f"\n→ Leyendo metadata: {metadata_obj['Key']}")
    response = s3.get_object(Bucket=S3_BUCKET_QUARANTINE, Key=metadata_obj["Key"])
    metadata = json.loads(response["Body"].read())

    print("\n" + "=" * 60)
    print(" CONTENIDO DEL METADATA.JSON")
    print("=" * 60)
    print(f"  Bucket original:  {metadata['original_bucket']}")
    print(f"  Key original:     {metadata['original_key']}")
    print(f"  Procesado en:     {metadata['processed_at']}")
    print(f"  Total líneas:     {metadata['total_lines']}")
    print(f"  Válidos:          {metadata['valid_records']}")
    print(f"  Inválidos:        {metadata['invalid_records']}")
    print(f"  Errores capturados (primeros 5):")
    for err in metadata["errors_sample"][:5]:
        print(f"    - línea {err['line']}: {err['errors']}")

    # 5. Aserciones finales
    print("\n" + "=" * 60)
    if (metadata["valid_records"] == expected_valid and
            metadata["invalid_records"] == expected_invalid):
        print(" ✓ TEST EXITOSO")
        print(f"   {metadata['valid_records']} válidos + {metadata['invalid_records']} inválidos = correcto")
    else:
        print(" ✗ TEST FALLIDO")
        print(f"   Esperado: {expected_valid} válidos, {expected_invalid} inválidos")
        print(f"   Obtuvo:   {metadata['valid_records']} válidos, {metadata['invalid_records']} inválidos")
        sys.exit(1)
    print("=" * 60)


if __name__ == "__main__":
    main()
