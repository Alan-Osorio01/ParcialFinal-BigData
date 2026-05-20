#!/usr/bin/env python3
"""
ShopStream — Generador de datos sintéticos
===========================================

Genera 5 entidades relacionadas (users, products, sessions, events, transactions)
con distribuciones realistas y las guarda como JSON Lines (.jsonl).

Uso:
    python punto1_datos/generator.py --date 2025-01-15 --events 500000
    python punto1_datos/generator.py --date 2025-01-15 --events 10000 --output ./test_data

Por defecto genera 500,000 eventos en data_output/.
"""
import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict

import numpy as np
from faker import Faker
from tqdm import tqdm

fake = Faker()
random.seed(42)
np.random.seed(42)
Faker.seed(42)

# ---------- Catálogos ----------
CATEGORIES = ["electronics", "clothing", "home", "sports", "beauty", "books", "toys"]
COUNTRIES = ["CO", "MX", "AR", "PE", "CL", "US", "BR", "EC"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
PAGE_TYPES = ["home", "category", "product", "cart", "checkout", "search"]
ELEMENT_TYPES = ["button", "link", "image", "banner", "product_card"]
REFERRERS = ["google.com", "facebook.com", "instagram.com", "direct", "tiktok.com", ""]

# Pesos realistas para distribución de eventos
EVENT_WEIGHTS = {
    "page_view": 0.40,
    "click": 0.25,
    "search": 0.10,
    "product_view": 0.15,
    "cart_event": 0.10,
}
DEVICE_WEIGHTS = {"mobile": 0.60, "desktop": 0.30, "tablet": 0.10}


# ---------- Generadores por entidad ----------
def generate_users(n: int) -> List[Dict]:
    users = []
    for _ in range(n):
        users.append({
            "user_id": str(uuid.uuid4()),
            "name": fake.name(),
            "email": fake.email(),
            "country": random.choice(COUNTRIES),
            "created_at": fake.date_time_between(start_date="-2y").isoformat(),
            "device_preference": random.choices(
                list(DEVICE_WEIGHTS.keys()), weights=list(DEVICE_WEIGHTS.values())
            )[0],
        })
    return users


def generate_products(n: int) -> List[Dict]:
    products = []
    for _ in range(n):
        # Precios con lognormal — mucha cola larga, más realista que uniform
        price = round(float(np.random.lognormal(3.5, 1.0)), 2)
        products.append({
            "product_id": str(uuid.uuid4()),
            "name": fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "price": price,
            "stock": random.randint(0, 1000),
            "created_at": fake.date_time_between(start_date="-1y").isoformat(),
        })
    return products


def generate_sessions(users: List[Dict], target_date: datetime, n: int) -> List[Dict]:
    sessions = []
    user_ids = [u["user_id"] for u in users]
    for _ in range(n):
        start_offset = random.randint(0, 86399 - 3600)
        duration = int(np.random.exponential(600))  # promedio 10 min
        started = target_date + timedelta(seconds=start_offset)
        ended = started + timedelta(seconds=duration)
        sessions.append({
            "session_id": str(uuid.uuid4()),
            "user_id": random.choice(user_ids),
            "started_at": started.isoformat(),
            "ended_at": ended.isoformat(),
            "device_type": random.choices(
                list(DEVICE_WEIGHTS.keys()), weights=list(DEVICE_WEIGHTS.values())
            )[0],
            "country": random.choice(COUNTRIES),
        })
    return sessions


def _build_event_base(user_id: str, session_id: str, event_type: str, timestamp: str) -> Dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "timestamp": timestamp,
    }


def _add_page_view_fields(base: Dict, device_type: str, country: str) -> Dict:
    page_type = random.choice(PAGE_TYPES)
    base.update({
        "page_url": f"/{page_type}/{fake.slug()}",
        "page_type": page_type,
        "time_on_page_seconds": int(np.random.exponential(120)),
        "referrer": random.choice(REFERRERS),
        "device_type": device_type,
        "country": country,
    })
    return base


def _add_click_fields(base: Dict) -> Dict:
    base.update({
        "element_id": f"elem_{random.randint(1, 500)}",
        "element_type": random.choice(ELEMENT_TYPES),
        "page_url": f"/{random.choice(PAGE_TYPES)}/{fake.slug()}",
        "x_position": random.randint(0, 1920),
        "y_position": random.randint(0, 1080),
    })
    return base


def _add_search_fields(base: Dict) -> Dict:
    base.update({
        "query": fake.words(nb=random.randint(1, 4), unique=True),
        "results_count": random.randint(0, 200),
    })
    return base


def _add_product_view_fields(base: Dict, products: List[Dict]) -> Dict:
    product = random.choice(products)
    base.update({
        "product_id": product["product_id"],
        "category": product["category"],
        "price": product["price"],
        "time_on_page_seconds": int(np.random.exponential(90)),
    })
    return base


def _add_cart_event_fields(base: Dict, products: List[Dict]) -> Dict:
    product = random.choice(products)
    base.update({
        "product_id": product["product_id"],
        "action": random.choices(["add", "remove"], weights=[0.75, 0.25])[0],
    })
    return base


def generate_events(
    users: List[Dict],
    products: List[Dict],
    sessions: List[Dict],
    target_date: datetime,
    n: int,
) -> List[Dict]:
    """Genera eventos asociados a sesiones reales (mantiene integridad referencial)."""
    events = []
    event_types = list(EVENT_WEIGHTS.keys())
    weights = list(EVENT_WEIGHTS.values())

    for _ in tqdm(range(n), desc="Eventos", unit=" ev"):
        session = random.choice(sessions)
        user_id = session["user_id"]
        session_id = session["session_id"]
        device_type = session["device_type"]
        country = session["country"]
        event_type = random.choices(event_types, weights=weights, k=1)[0]

        ts_offset = random.randint(0, 86399)
        timestamp = (target_date + timedelta(seconds=ts_offset)).isoformat()
        base = _build_event_base(user_id, session_id, event_type, timestamp)

        if event_type == "page_view":
            events.append(_add_page_view_fields(base, device_type, country))
        elif event_type == "click":
            events.append(_add_click_fields(base))
        elif event_type == "search":
            events.append(_add_search_fields(base))
        elif event_type == "product_view":
            events.append(_add_product_view_fields(base, products))
        elif event_type == "cart_event":
            events.append(_add_cart_event_fields(base, products))

    return events


def generate_transactions(
    users: List[Dict], products: List[Dict], sessions: List[Dict],
    target_date: datetime, n: int,
) -> List[Dict]:
    transactions = []
    for _ in range(n):
        product = random.choice(products)
        session = random.choice(sessions)
        transactions.append({
            "tx_id": str(uuid.uuid4()),
            "user_id": session["user_id"],
            "session_id": session["session_id"],
            "product_id": product["product_id"],
            "amount": product["price"],
            "status": random.choices(
                ["completed", "failed", "pending"], weights=[0.70, 0.10, 0.20]
            )[0],
            "created_at": (
                target_date + timedelta(seconds=random.randint(0, 86399))
            ).isoformat(),
        })
    return transactions


def write_jsonl(path: str, data: List[Dict]) -> None:
    with open(path, "w") as f:
        for row in data:
            f.write(json.dumps(row) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--events", type=int, default=500_000,
                        help="Cantidad de eventos a generar (default 500k)")
    parser.add_argument("--users", type=int, default=5_000)
    parser.add_argument("--products", type=int, default=500)
    parser.add_argument("--sessions", type=int, default=20_000)
    parser.add_argument("--transactions", type=int, default=5_000)
    parser.add_argument("--output", default="./data_output")
    args = parser.parse_args()

    target = datetime.strptime(args.date, "%Y-%m-%d")
    os.makedirs(args.output, exist_ok=True)

    print(f"Generando datos para {args.date} → {args.output}/")

    print(f"  Usuarios:      {args.users:,}")
    users = generate_users(args.users)

    print(f"  Productos:     {args.products:,}")
    products = generate_products(args.products)

    print(f"  Sesiones:      {args.sessions:,}")
    sessions = generate_sessions(users, target, args.sessions)

    print(f"  Eventos:       {args.events:,}")
    events = generate_events(users, products, sessions, target, args.events)

    print(f"  Transacciones: {args.transactions:,}")
    transactions = generate_transactions(users, products, sessions, target, args.transactions)

    # Guardar
    for name, data in [
        ("users", users),
        ("products", products),
        ("sessions", sessions),
        ("events", events),
        ("transactions", transactions),
    ]:
        path = os.path.join(args.output, f"{name}.jsonl")
        write_jsonl(path, data)
        size_mb = os.path.getsize(path) / 1024 / 1024
        print(f"  ✓ {name:13s} → {path}  ({len(data):>8,} regs, {size_mb:.1f} MB)")

    print("\nDone.")


if __name__ == "__main__":
    main()
