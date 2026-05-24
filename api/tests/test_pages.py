import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest.mock as mock


def test_pages_top_missing_date():
    """Sin 'date' debe retornar 400."""
    from app import app
    with app.test_client() as client:
        resp = client.get("/pages/top?metric=time_on_page")
        assert resp.status_code == 400
        data = resp.get_json()
        assert "error" in data


def test_pages_top_invalid_metric():
    """Métrica inválida debe retornar 400."""
    from app import app
    with app.test_client() as client:
        resp = client.get("/pages/top?metric=INVALID&date=2025-01-15")
        assert resp.status_code == 400


def test_pages_top_valid_time_on_page():
    """Solicitud válida con mock de BD debe retornar 200."""
    from app import app
    mock_rows = [
        {"page_url": "/home", "page_type": "home", "avg_time_seconds": 120.5,
         "total_views": 1000, "bounce_rate": 45.2, "date": "2025-01-15"}
    ]
    with app.test_client() as client:
        with mock.patch("handlers.pages.query", return_value=mock_rows):
            resp = client.get("/pages/top?metric=time_on_page&date=2025-01-15&limit=5")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["count"] == 1
            assert data["metric"] == "time_on_page"


def test_pages_top_bounce_rate_metric():
    """Debe aceptar metric=bounce_rate."""
    from app import app
    with app.test_client() as client:
        with mock.patch("handlers.pages.query", return_value=[]):
            resp = client.get("/pages/top?metric=bounce_rate&date=2025-01-15")
            assert resp.status_code == 200


def test_pages_top_returns_json():
    """La respuesta debe ser JSON."""
    from app import app
    with app.test_client() as client:
        with mock.patch("handlers.pages.query", return_value=[]):
            resp = client.get("/pages/top?metric=time_on_page&date=2025-01-15")
            assert resp.content_type == "application/json"