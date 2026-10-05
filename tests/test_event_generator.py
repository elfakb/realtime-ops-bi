from datetime import datetime

from src.producer.event_generator import build_event, init_catalog


def test_build_event_has_envelope_fields():
    ev = build_event("ORDER_CREATED", order_id=1, amount=100.0)
    assert ev["event_type"] == "ORDER_CREATED"
    assert ev["order_id"] == 1
    assert len(ev["event_id"]) == 36
    datetime.fromisoformat(ev["event_time"])  # must be valid ISO-8601


def test_event_ids_are_unique():
    ids = {build_event("PAGE_VIEW")["event_id"] for _ in range(1000)}
    assert len(ids) == 1000


def test_catalog_shape():
    catalog = init_catalog(50)
    assert len(catalog) == 50
    for item in catalog.values():
        assert item["stock"] > item["reorder_point"]
        assert item["price"] > 0