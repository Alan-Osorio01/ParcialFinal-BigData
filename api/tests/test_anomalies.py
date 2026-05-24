import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest.mock as mock


def test_anomalies_missing_date():
    """Sin 'date' debe retornar 400."""
    from app import app
    with app.test_client() as client:
        resp = client.get("/anomalies")
        assert resp.status_code == 400


def test_anomalies_returns_list():
    """Con date válida y mock debe retornar datos correctamente."""
    from app import app
    mock_rows = [
        {"session_id": "s1", "user_id": "u1", "total_time": 9999.5,
         "event_count": 500, "zscore_time": 4.2, "anomaly_type": "zscore_time",
         "date": "2025-01-15"}
    ]
    with app.test_client() as client:
        with mock.patch("handlers.anomalies.query", return_value=mock_rows):
            resp = client.get("/anomalies?date=2025-01-15")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["total_anomalies"] == 1
            assert data["data"][0]["zscore_time"] == 4.2


def test_anomalies_empty_result():
    """Si no hay anomalías retorna lista vacía con 200."""
    from app import app
    with app.test_client() as client:
        with mock.patch("handlers.anomalies.query", return_value=[]):
            resp = client.get("/anomalies?date=2099-01-01")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["total_anomalies"] == 0


def test_health_check():
    """Health check siempre retorna 200 con status ok."""
    from app import app
    with app.test_client() as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "ok"