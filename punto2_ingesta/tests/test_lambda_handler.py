"""Tests del handler Lambda usando moto para mockear S3 y CloudWatch."""
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# Marcamos como skip si moto no está instalado (no es dependencia obligatoria del CI base).
moto = pytest.importorskip("moto")
from moto import mock_aws  # noqa: E402
import boto3  # noqa: E402


RAW_BUCKET = "shopstream-raw-aad"
QUARANTINE_BUCKET = "shopstream-quarantine-aad"


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("QUARANTINE_BUCKET", QUARANTINE_BUCKET)
    monkeypatch.setenv("CLOUDWATCH_NAMESPACE", "ShopStream/Test")


def _make_event(bucket: str, key: str, size: int) -> dict:
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key, "size": size},
                }
            }
        ]
    }


def _setup_buckets():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=RAW_BUCKET)
    s3.create_bucket(Bucket=QUARANTINE_BUCKET)
    return s3


@mock_aws
def test_valid_events_file_does_not_quarantine():
    # Importar dentro del mock para que el cliente boto3 use el mock
    import importlib
    import lambda_function
    importlib.reload(lambda_function)

    s3 = _setup_buckets()

    valid_events = [
        {"event_id": "1", "event_type": "page_view", "user_id": "u1",
         "session_id": "s1", "timestamp": "2025-01-15T10:00:00",
         "page_url": "/home", "page_type": "home", "time_on_page_seconds": 30,
         "device_type": "mobile", "country": "CO"},
        {"event_id": "2", "event_type": "click", "user_id": "u1",
         "session_id": "s1", "timestamp": "2025-01-15T10:01:00",
         "element_id": "btn", "element_type": "button",
         "page_url": "/home", "x_position": 100, "y_position": 200},
    ]
    body = "\n".join(json.dumps(e) for e in valid_events)
    key = "year=2025/month=01/day=15/events.jsonl"
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=body.encode("utf-8"))

    event = _make_event(RAW_BUCKET, key, len(body))
    response = lambda_function.lambda_handler(event, context=None)

    assert response["statusCode"] == 200
    body_resp = json.loads(response["body"])
    result = body_resp["results"][0]
    assert result["status"] == "ok"
    assert result["valid"] == 2

    # No debe haber nada en quarantine
    listing = s3.list_objects_v2(Bucket=QUARANTINE_BUCKET)
    assert listing.get("KeyCount", 0) == 0


@mock_aws
def test_invalid_events_go_to_quarantine():
    import importlib
    import lambda_function
    importlib.reload(lambda_function)

    s3 = _setup_buckets()

    mixed = [
        # válido
        {"event_id": "1", "event_type": "page_view", "user_id": "u1",
         "session_id": "s1", "timestamp": "2025-01-15T10:00:00",
         "page_url": "/home", "page_type": "home", "time_on_page_seconds": 30,
         "device_type": "mobile", "country": "CO"},
        # inválido: precio negativo
        {"event_id": "2", "event_type": "product_view", "user_id": "u1",
         "session_id": "s1", "timestamp": "2025-01-15T10:01:00",
         "product_id": "p1", "category": "electronics",
         "price": -50, "time_on_page_seconds": 10},
    ]
    body = "\n".join(json.dumps(e) for e in mixed)
    key = "year=2025/month=01/day=15/events.jsonl"
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=body.encode("utf-8"))

    event = _make_event(RAW_BUCKET, key, len(body))
    response = lambda_function.lambda_handler(event, context=None)

    assert response["statusCode"] == 200
    result = json.loads(response["body"])["results"][0]
    assert result["status"] == "quarantined"
    assert result["valid"] == 1
    assert result["invalid"] == 1

    # Debe haber 2 archivos en quarantine: el original copiado + metadata.json
    listing = s3.list_objects_v2(Bucket=QUARANTINE_BUCKET)
    keys = [obj["Key"] for obj in listing.get("Contents", [])]
    assert any(k.endswith(".metadata.json") for k in keys)
    assert any(k.endswith("events.jsonl") for k in keys)


@mock_aws
def test_non_event_file_is_skipped():
    import importlib
    import lambda_function
    importlib.reload(lambda_function)

    s3 = _setup_buckets()
    key = "year=2025/month=01/day=15/users.jsonl"  # no es events.jsonl
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=b'{"any": "thing"}')

    event = _make_event(RAW_BUCKET, key, 16)
    response = lambda_function.lambda_handler(event, context=None)

    result = json.loads(response["body"])["results"][0]
    assert result["status"] == "skipped"


@mock_aws
def test_malformed_json_line_goes_to_quarantine():
    import importlib
    import lambda_function
    importlib.reload(lambda_function)

    s3 = _setup_buckets()

    body = (
        json.dumps({"event_id": "1", "event_type": "page_view", "user_id": "u1",
                    "session_id": "s1", "timestamp": "2025-01-15T10:00:00",
                    "page_url": "/home", "page_type": "home",
                    "time_on_page_seconds": 30, "device_type": "mobile",
                    "country": "CO"})
        + "\n"
        + "{esto no es JSON válido}"
    )
    key = "year=2025/month=01/day=15/events.jsonl"
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=body.encode("utf-8"))

    event = _make_event(RAW_BUCKET, key, len(body))
    response = lambda_function.lambda_handler(event, context=None)
    result = json.loads(response["body"])["results"][0]
    assert result["status"] == "quarantined"
    assert result["invalid"] == 1
