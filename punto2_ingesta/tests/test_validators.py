"""Tests del módulo de validación de eventos."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from validators import validate_event


def _base(event_type: str, **extra) -> dict:
    """Construye un evento válido del tipo indicado para tests."""
    template = {
        "event_id": "evt-1",
        "event_type": event_type,
        "user_id": "user-1",
        "session_id": "session-1",
        "timestamp": "2025-01-15T10:30:00",
    }
    template.update(extra)
    return template


# ---------- Casos válidos ----------
def test_valid_page_view():
    evt = _base("page_view",
                page_url="/home",
                page_type="home",
                time_on_page_seconds=30,
                device_type="mobile",
                country="CO")
    assert validate_event(evt) == []


def test_valid_click():
    evt = _base("click",
                element_id="btn-1",
                element_type="button",
                page_url="/cart",
                x_position=100,
                y_position=200)
    assert validate_event(evt) == []


def test_valid_search():
    evt = _base("search", query=["zapatos", "deportivos"], results_count=42)
    assert validate_event(evt) == []


def test_valid_product_view():
    evt = _base("product_view",
                product_id="p1",
                category="electronics",
                price=99.99,
                time_on_page_seconds=60)
    assert validate_event(evt) == []


def test_valid_cart_event_add():
    evt = _base("cart_event", product_id="p1", action="add")
    assert validate_event(evt) == []


def test_valid_cart_event_remove():
    evt = _base("cart_event", product_id="p1", action="remove")
    assert validate_event(evt) == []


# ---------- Casos inválidos ----------
def test_invalid_event_type():
    evt = {"event_type": "UNKNOWN"}
    errors = validate_event(evt)
    assert any("event_type" in e for e in errors)


def test_missing_event_type():
    evt = {"event_id": "1"}
    errors = validate_event(evt)
    assert len(errors) > 0


def test_missing_required_field_page_view():
    evt = _base("page_view",
                page_url="/home",
                page_type="home",
                time_on_page_seconds=30,
                device_type="mobile")
    # falta 'country'
    errors = validate_event(evt)
    assert any("country" in e for e in errors)


def test_invalid_timestamp():
    evt = _base("page_view",
                timestamp="not-a-date",
                page_url="/home",
                page_type="home",
                time_on_page_seconds=10,
                device_type="mobile",
                country="MX")
    errors = validate_event(evt)
    assert any("Timestamp" in e or "timestamp" in e for e in errors)


def test_invalid_device_type():
    evt = _base("page_view",
                page_url="/home",
                page_type="home",
                time_on_page_seconds=10,
                device_type="smartwatch",  # inválido
                country="CO")
    errors = validate_event(evt)
    assert any("device_type" in e for e in errors)


def test_invalid_page_type():
    evt = _base("page_view",
                page_url="/foo",
                page_type="UNKNOWN_PAGE",
                time_on_page_seconds=10,
                device_type="mobile",
                country="CO")
    errors = validate_event(evt)
    assert any("page_type" in e for e in errors)


def test_negative_time_on_page():
    evt = _base("page_view",
                page_url="/home",
                page_type="home",
                time_on_page_seconds=-5,
                device_type="mobile",
                country="CO")
    errors = validate_event(evt)
    assert any("time_on_page_seconds" in e for e in errors)


def test_negative_price():
    evt = _base("product_view",
                product_id="p1",
                category="electronics",
                price=-10.0,
                time_on_page_seconds=20)
    errors = validate_event(evt)
    assert any("price" in e for e in errors)


def test_zero_price():
    evt = _base("product_view",
                product_id="p1",
                category="electronics",
                price=0,
                time_on_page_seconds=20)
    errors = validate_event(evt)
    assert any("price" in e for e in errors)


def test_negative_results_count():
    evt = _base("search", query=["test"], results_count=-1)
    errors = validate_event(evt)
    assert any("results_count" in e for e in errors)


def test_invalid_cart_action():
    evt = _base("cart_event", product_id="p1", action="delete")
    errors = validate_event(evt)
    assert any("action" in e for e in errors)


def test_negative_click_position():
    evt = _base("click",
                element_id="btn",
                element_type="button",
                page_url="/cart",
                x_position=-5,
                y_position=100)
    errors = validate_event(evt)
    assert any("x_position" in e for e in errors)


def test_not_a_dict_returns_error():
    errors = validate_event("not a dict")
    assert len(errors) > 0
