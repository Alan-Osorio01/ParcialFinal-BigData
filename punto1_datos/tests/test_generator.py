"""Tests del generador de datos sintéticos."""
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from generator import (
    generate_users,
    generate_products,
    generate_sessions,
    generate_events,
    generate_transactions,
    DEVICE_TYPES,
    PAGE_TYPES,
    EVENT_WEIGHTS,
)


@pytest.fixture
def target_date():
    return datetime(2025, 1, 15)


@pytest.fixture
def users():
    return generate_users(20)


@pytest.fixture
def products():
    return generate_products(15)


@pytest.fixture
def sessions(users, target_date):
    return generate_sessions(users, target_date, 30)


# ---------- Users ----------
def test_users_count():
    assert len(generate_users(50)) == 50


def test_users_fields():
    u = generate_users(1)[0]
    assert {"user_id", "name", "email", "country",
            "created_at", "device_preference"} <= u.keys()
    assert u["device_preference"] in DEVICE_TYPES
    assert len(u["country"]) == 2


def test_users_emails_have_at_sign(users):
    assert all("@" in u["email"] for u in users)


# ---------- Products ----------
def test_products_price_positive(products):
    assert all(p["price"] > 0 for p in products)


def test_products_stock_non_negative(products):
    assert all(p["stock"] >= 0 for p in products)


def test_products_have_category(products):
    assert all("category" in p and p["category"] for p in products)


# ---------- Sessions ----------
def test_sessions_count(sessions):
    assert len(sessions) == 30


def test_sessions_user_ids_belong_to_users(users, sessions):
    user_ids = {u["user_id"] for u in users}
    assert all(s["user_id"] in user_ids for s in sessions)


def test_sessions_ended_after_started(sessions):
    for s in sessions:
        started = datetime.fromisoformat(s["started_at"])
        ended = datetime.fromisoformat(s["ended_at"])
        assert ended >= started


# ---------- Events ----------
def test_events_types_are_valid(users, products, sessions, target_date):
    events = generate_events(users, products, sessions, target_date, 200)
    valid_types = set(EVENT_WEIGHTS.keys())
    assert all(e["event_type"] in valid_types for e in events)


def test_events_have_required_base_fields(users, products, sessions, target_date):
    events = generate_events(users, products, sessions, target_date, 100)
    for e in events:
        assert {"event_id", "event_type", "user_id",
                "session_id", "timestamp"} <= e.keys()


def test_page_view_has_page_type(users, products, sessions, target_date):
    events = generate_events(users, products, sessions, target_date, 500)
    page_views = [e for e in events if e["event_type"] == "page_view"]
    assert all(e["page_type"] in PAGE_TYPES for e in page_views)
    assert all(e["device_type"] in DEVICE_TYPES for e in page_views)


def test_cart_event_action_valid(users, products, sessions, target_date):
    events = generate_events(users, products, sessions, target_date, 1000)
    cart_events = [e for e in events if e["event_type"] == "cart_event"]
    assert all(e["action"] in ("add", "remove") for e in cart_events)


def test_product_view_has_valid_price(users, products, sessions, target_date):
    events = generate_events(users, products, sessions, target_date, 500)
    pvs = [e for e in events if e["event_type"] == "product_view"]
    assert all(e["price"] > 0 for e in pvs)


def test_event_session_belongs_to_sessions(users, products, sessions, target_date):
    events = generate_events(users, products, sessions, target_date, 100)
    valid_sessions = {s["session_id"] for s in sessions}
    assert all(e["session_id"] in valid_sessions for e in events)


# ---------- Transactions ----------
def test_transactions_status_valid(users, products, sessions, target_date):
    txs = generate_transactions(users, products, sessions, target_date, 50)
    assert all(t["status"] in ("completed", "failed", "pending") for t in txs)


def test_transactions_amount_matches_product_price(users, products, sessions, target_date):
    txs = generate_transactions(users, products, sessions, target_date, 50)
    product_prices = {p["product_id"]: p["price"] for p in products}
    for t in txs:
        assert t["amount"] == product_prices[t["product_id"]]
