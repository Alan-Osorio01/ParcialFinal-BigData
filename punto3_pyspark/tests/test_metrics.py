import pytest
from pyspark.sql import SparkSession
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from transformations.metrics import (
    metric_top_pages_time,
    metric_bounce_rate,
    metric_products_view_vs_cart,
    metric_navigation_paths,
    metric_time_by_device_country,
)


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("test-metrics")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


@pytest.fixture
def sample_events(spark):
    data = [
        {"event_type": "page_view",    "page_url": "/home",      "page_type": "home",
         "time_on_page_seconds": 120,  "session_id": "s1",       "user_id": "u1",
         "date": "2025-01-15",         "device_type": "mobile",  "country": "CO",
         "timestamp": datetime(2025, 1, 15, 10, 0, 0)},
        {"event_type": "page_view",    "page_url": "/home",      "page_type": "home",
         "time_on_page_seconds": 60,   "session_id": "s2",       "user_id": "u2",
         "date": "2025-01-15",         "device_type": "desktop", "country": "MX",
         "timestamp": datetime(2025, 1, 15, 10, 1, 0)},
        {"event_type": "page_view",    "page_url": "/product/1", "page_type": "product",
         "time_on_page_seconds": 300,  "session_id": "s3",       "user_id": "u3",
         "date": "2025-01-15",         "device_type": "tablet",  "country": "CO",
         "timestamp": datetime(2025, 1, 15, 10, 2, 0)},
        {"event_type": "product_view", "page_url": "/product/1", "page_type": "product",
         "time_on_page_seconds": 200,  "session_id": "s1",       "user_id": "u1",
         "date": "2025-01-15",         "device_type": "mobile",  "country": "CO",
         "product_id": "p1",           "category": "electronics",
         "timestamp": datetime(2025, 1, 15, 10, 3, 0)},
        {"event_type": "cart_event",   "page_url": "/cart",      "page_type": "cart",
         "time_on_page_seconds": 50,   "session_id": "s1",       "user_id": "u1",
         "date": "2025-01-15",         "device_type": "mobile",  "country": "CO",
         "product_id": "p1",           "action": "add",
         "timestamp": datetime(2025, 1, 15, 10, 4, 0)},
    ]
    return spark.createDataFrame(data)


# --- Métrica 1 ---
def test_top_pages_time_retorna_filas(sample_events):
    result = metric_top_pages_time(sample_events)
    assert result.count() > 0


def test_top_pages_time_tiene_columnas(sample_events):
    result  = metric_top_pages_time(sample_events)
    columns = result.columns
    assert "avg_time_seconds" in columns
    assert "page_url"         in columns
    assert "total_views"      in columns


# --- Métrica 2 ---
def test_bounce_rate_rango_valido(sample_events):
    result = metric_bounce_rate(sample_events)
    rows   = result.collect()
    for row in rows:
        assert 0 <= row["bounce_rate"] <= 100


def test_bounce_rate_tiene_columnas(sample_events):
    result  = metric_bounce_rate(sample_events)
    columns = result.columns
    assert "bounce_rate"      in columns
    assert "total_sessions"   in columns
    assert "bounced_sessions" in columns


# --- Métrica 4 ---
def test_products_view_vs_cart_retorna_filas(sample_events):
    result = metric_products_view_vs_cart(sample_events)
    assert result.count() > 0


def test_products_view_vs_cart_tiene_ratio(sample_events):
    result  = metric_products_view_vs_cart(sample_events)
    columns = result.columns
    assert "view_to_cart_ratio"  in columns
    assert "high_view_low_cart"  in columns


# --- Métrica 5 ---
def test_navigation_paths_retorna_filas(sample_events):
    result = metric_navigation_paths(sample_events)
    assert result.count() > 0


def test_navigation_paths_limite_10(sample_events):
    result = metric_navigation_paths(sample_events)
    assert result.count() <= 10


# --- Métrica 6 ---
def test_time_by_device_country_retorna_filas(sample_events):
    result = metric_time_by_device_country(sample_events)
    assert result.count() > 0


def test_time_by_device_country_tiene_columnas(sample_events):
    result  = metric_time_by_device_country(sample_events)
    columns = result.columns
    assert "avg_time_seconds" in columns
    assert "device_type"      in columns
    assert "country"          in columns