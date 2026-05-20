"""
Validadores de eventos ShopStream.
Sin dependencias externas (se empaqueta dentro del .zip de Lambda).
"""
from datetime import datetime
from typing import List

REQUIRED_FIELDS = {
    "page_view": ["event_id", "user_id", "session_id", "timestamp", "page_url",
                  "page_type", "time_on_page_seconds", "device_type", "country"],
    "click": ["event_id", "user_id", "session_id", "timestamp", "element_id",
              "element_type", "page_url", "x_position", "y_position"],
    "search": ["event_id", "user_id", "session_id", "timestamp", "query", "results_count"],
    "product_view": ["event_id", "user_id", "session_id", "timestamp", "product_id",
                     "category", "price", "time_on_page_seconds"],
    "cart_event": ["event_id", "user_id", "session_id", "timestamp", "product_id", "action"],
}

VALID_DEVICES = {"mobile", "desktop", "tablet"}
VALID_PAGE_TYPES = {"home", "category", "product", "cart", "checkout", "search"}
VALID_CART_ACTIONS = {"add", "remove"}


def _validate_timestamp(ts) -> bool:
    if not isinstance(ts, str):
        return False
    try:
        datetime.fromisoformat(ts)
        return True
    except (ValueError, TypeError):
        return False


def validate_event(event: dict) -> List[str]:
    """Retorna lista de errores. Si la lista está vacía, el evento es válido."""
    errors: List[str] = []

    if not isinstance(event, dict):
        return ["Evento no es un objeto JSON"]

    event_type = event.get("event_type")
    if event_type not in REQUIRED_FIELDS:
        return [f"event_type inválido: {event_type!r}"]

    # Campos requeridos
    for field in REQUIRED_FIELDS[event_type]:
        if field not in event or event[field] is None:
            errors.append(f"Campo requerido faltante: {field}")

    # Timestamp
    if "timestamp" in event and not _validate_timestamp(event["timestamp"]):
        errors.append(f"Timestamp inválido: {event.get('timestamp')!r}")

    # Validaciones específicas por tipo
    if event_type == "page_view":
        tsp = event.get("time_on_page_seconds")
        if tsp is not None and (not isinstance(tsp, (int, float)) or tsp < 0):
            errors.append(f"time_on_page_seconds inválido: {tsp!r}")
        dev = event.get("device_type")
        if dev is not None and dev not in VALID_DEVICES:
            errors.append(f"device_type inválido: {dev!r}")
        pt = event.get("page_type")
        if pt is not None and pt not in VALID_PAGE_TYPES:
            errors.append(f"page_type inválido: {pt!r}")

    elif event_type == "click":
        for axis in ("x_position", "y_position"):
            v = event.get(axis)
            if v is not None and (not isinstance(v, (int, float)) or v < 0):
                errors.append(f"{axis} inválido: {v!r}")

    elif event_type == "search":
        rc = event.get("results_count")
        if rc is not None and (not isinstance(rc, int) or rc < 0):
            errors.append(f"results_count inválido: {rc!r}")

    elif event_type == "product_view":
        price = event.get("price")
        if price is not None and (not isinstance(price, (int, float)) or price <= 0):
            errors.append(f"price inválido: {price!r}")
        tsp = event.get("time_on_page_seconds")
        if tsp is not None and (not isinstance(tsp, (int, float)) or tsp < 0):
            errors.append(f"time_on_page_seconds inválido: {tsp!r}")

    elif event_type == "cart_event":
        action = event.get("action")
        if action is not None and action not in VALID_CART_ACTIONS:
            errors.append(f"action inválido: {action!r}")

    return errors
