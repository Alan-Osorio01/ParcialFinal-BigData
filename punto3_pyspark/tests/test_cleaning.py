import pytest
from pyspark.sql import SparkSession
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from transformations.cleaning import deduplicate, impute_nulls, filter_valid_events


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("test-cleaning")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def test_deduplicate(spark):
    data = [
        {"event_id": "1", "event_type": "page_view"},
        {"event_id": "1", "event_type": "page_view"},  # duplicado
        {"event_id": "2", "event_type": "click"},
    ]
    df     = spark.createDataFrame(data)
    result = deduplicate(df)
    assert result.count() == 2


def test_deduplicate_sin_duplicados(spark):
    data = [
        {"event_id": "1", "event_type": "page_view"},
        {"event_id": "2", "event_type": "click"},
        {"event_id": "3", "event_type": "search"},
    ]
    df     = spark.createDataFrame(data)
    result = deduplicate(df)
    assert result.count() == 3


def test_impute_nulls(spark):
    data = [
        {"event_id": "1", "time_on_page_seconds": None, "country": None, "device_type": None},
        {"event_id": "2", "time_on_page_seconds": 30,   "country": "CO", "device_type": "mobile"},
    ]
    df     = spark.createDataFrame(data)
    result = impute_nulls(df)
    rows   = {r["event_id"]: r for r in result.collect()}
    assert rows["1"]["time_on_page_seconds"] == 0
    assert rows["1"]["country"]              == "unknown"
    assert rows["1"]["device_type"]          == "unknown"
    assert rows["2"]["country"]              == "CO"


def test_filter_valid_events(spark):
    data = [
        {"event_id": "1", "event_type": "page_view", "user_id": "u1",
         "session_id": "s1", "timestamp": datetime(2025, 1, 15, 10, 0, 0)},
        {"event_id": "2", "event_type": "INVALID",   "user_id": "u2",
         "session_id": "s2", "timestamp": datetime(2025, 1, 15, 11, 0, 0)},
        {"event_id": "3", "event_type": "click",     "user_id": None,
         "session_id": "s3", "timestamp": datetime(2025, 1, 15, 12, 0, 0)},
    ]
    df     = spark.createDataFrame(data)
    result = filter_valid_events(df)
    assert result.count() == 1


def test_filter_valid_events_todos_validos(spark):
    data = [
        {"event_id": "1", "event_type": "page_view",    "user_id": "u1",
         "session_id": "s1", "timestamp": datetime(2025, 1, 15, 10, 0, 0)},
        {"event_id": "2", "event_type": "product_view", "user_id": "u2",
         "session_id": "s2", "timestamp": datetime(2025, 1, 15, 11, 0, 0)},
        {"event_id": "3", "event_type": "cart_event",   "user_id": "u3",
         "session_id": "s3", "timestamp": datetime(2025, 1, 15, 12, 0, 0)},
    ]
    df     = spark.createDataFrame(data)
    result = filter_valid_events(df)
    assert result.count() == 3