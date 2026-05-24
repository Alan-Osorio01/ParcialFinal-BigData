import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest.mock as mock


def test_sessions_missing_date():
    """Sin 'date' debe retornar 400."""
    from app import app
    with app.test_client() as client:
        resp = client.get("/sessions/summary")
        assert resp.status_code == 400


def test_sessions_with_all_filters():
    """Con country, device y date debe retornar 200."""
    from app import app
    mock_rows = [
        {"device_type": "mobile", "country": "CO", "avg_time_seconds": 95.3,
         "total_views": 500, "stddev_time": 30.1, "date": "2025-01-15"}
    ]
    with app.test_client() as client:
        with mock.patch("handlers.sessions.query", return_value=mock_rows):
            resp = client.get("/sessions/summary?country=CO&device=mobile&date=2025-01-15")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["country"] == "CO"
            assert data["device"] == "mobile"
            assert data["count"] == 1


def test_sessions_without_optional_filters():
    """Solo con date debe funcionar."""
    from app import app
    with app.test_client() as client:
        with mock.patch("handlers.sessions.query", return_value=[]):
            resp = client.get("/sessions/summary?date=2025-01-15")
            assert resp.status_code == 200


def test_sessions_response_has_count():
    """La respuesta debe incluir el campo 'count'."""
    from app import app
    with app.test_client() as client:
        with mock.patch("handlers.sessions.query", return_value=[]):
            resp = client.get("/sessions/summary?date=2025-01-15")
            data = resp.get_json()
            assert "count" in data