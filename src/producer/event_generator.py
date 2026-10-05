"""
Simulated e-commerce event generator -> Kafka.

Order lifecycle:
  ORDER_CREATED -> PAYMENT_COMPLETED -> ORDER_SHIPPED -> ORDER_DELIVERED
                \-> ORDER_CANCELLED
Plus INVENTORY_UPDATED and PAGE_VIEW events.

Run:  python -m src.producer.event_generator
"""
import heapq
import itertools
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from src.common.config import (
    DEMO_SPEEDUP,
    INCIDENTS,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    ORDERS_PER_MINUTE,
    PAGEVIEWS_PER_ORDER,
)

CITIES = ["Istanbul", "Ankara", "Izmir", "Bursa", "Antalya", "Adana", "Konya", "Gaziantep"]
CANCEL_REASONS = ["PAYMENT_FAILED", "CUSTOMER_REQUEST", "OUT_OF_STOCK", "FRAUD_SUSPECTED"]
RESTOCK_QTY = 150


def build_event(event_type: str, **fields) -> dict:
    """Create an event envelope with a unique id and a UTC timestamp."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": datetime.now(timezone.utc).isoformat(),
        **fields,
    }


def init_catalog(n: int = 50, rng=random) -> dict:
    return {
        f"SKU-{i:04d}": {
            "price": round(rng.uniform(49, 4999), 2),
            "stock": rng.randint(60, 200),
            "reorder_point": rng.randint(15, 30),
            "restock_pending": False,
        }
        for i in range(1, n + 1)
    }


class Simulator:
    def __init__(self) -> None:
        self.catalog = init_catalog()
        self.heap: list = []
        self.seq = itertools.count()
        self.order_id = time.time_ns() // 1_000_000  # ms-based, avoids collisions on restart
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 50})

    # ---------- infrastructure ----------
    @staticmethod
    def _delay(lo: float, hi: float) -> float:
        return random.uniform(lo, hi) / DEMO_SPEEDUP

    def schedule(self, delay: float, kind: str, **data) -> None:
        heapq.heappush(self.heap, (time.time() + delay, next(self.seq), kind, data))

    def emit(self, event: dict, key) -> None:
        self.producer.produce(KAFKA_TOPIC, key=str(key), value=json.dumps(event))
        self.producer.poll(0)

    # ---------- business logic ----------
    @staticmethod
    def cancel_probability() -> float:
        # Simulated incident: 2 minutes out of every 15 have a high cancellation rate.
        if INCIDENTS and (time.time() % 900) < 120:
            return 0.20
        return 0.03

    def update_stock(self, sku: str, delta: int) -> None:
        item = self.catalog[sku]
        item["stock"] = max(0, item["stock"] + delta)
        self.emit(
            build_event(
                "INVENTORY_UPDATED",
                sku=sku,
                stock_level=item["stock"],
                reorder_point=item["reorder_point"],
            ),
            sku,
        )
        if item["stock"] <= item["reorder_point"] and not item["restock_pending"]:
            item["restock_pending"] = True
            self.schedule(self._delay(240, 600), "RESTOCK", sku=sku)

    def publish_initial_inventory(self) -> None:
        for sku, item in self.catalog.items():
            self.emit(
                build_event(
                    "INVENTORY_UPDATED",
                    sku=sku,
                    stock_level=item["stock"],
                    reorder_point=item["reorder_point"],
                ),
                sku,
            )

    def create_order(self) -> None:
        sku = random.choice(list(self.catalog))
        item = self.catalog[sku]
        if item["stock"] <= 0:
            return  # out of stock, no order

        qty = min(random.randint(1, 3), item["stock"])
        self.order_id += 1
        oid = self.order_id
        amount = round(item["price"] * qty, 2)

        self.emit(
            build_event(
                "ORDER_CREATED",
                order_id=oid,
                customer_id=random.randint(1, 50_000),
                sku=sku,
                quantity=qty,
                amount=amount,
                city=random.choice(CITIES),
            ),
            oid,
        )
        self.update_stock(sku, -qty)

        # Cancelled path
        if random.random() < self.cancel_probability():
            if random.random() < 0.5:
                self.schedule(self._delay(3, 60), "PAYMENT_COMPLETED", order_id=oid, amount=amount)
            self.schedule(
                self._delay(60, 180),
                "ORDER_CANCELLED",
                order_id=oid,
                cancel_reason=random.choice(CANCEL_REASONS),
            )
            return

        # Happy path (6 % of orders are slow -> SLA breaches)
        slow = random.random() < 0.06
        t_pay = self._delay(3, 90)
        t_ship = t_pay + (self._delay(540, 840) if slow else self._delay(60, 360))
        t_deliver = t_ship + (self._delay(1500, 2100) if slow else self._delay(480, 1200))

        self.schedule(t_pay, "PAYMENT_COMPLETED", order_id=oid, amount=amount)
        self.schedule(t_ship, "ORDER_SHIPPED", order_id=oid)
        self.schedule(t_deliver, "ORDER_DELIVERED", order_id=oid)

    def emit_page_views(self) -> None:
        for _ in range(random.randint(1, max(1, 2 * PAGEVIEWS_PER_ORDER))):
            ev = build_event("PAGE_VIEW")
            self.emit(ev, ev["event_id"])

    def dispatch_due(self) -> None:
        now = time.time()
        while self.heap and self.heap[0][0] <= now:
            _, _, kind, data = heapq.heappop(self.heap)
            if kind == "RESTOCK":
                self.catalog[data["sku"]]["restock_pending"] = False
                self.update_stock(data["sku"], RESTOCK_QTY)
            else:
                self.emit(build_event(kind, **data), data["order_id"])

    # ---------- main loop ----------
    def run(self) -> None:
        rate_per_sec = ORDERS_PER_MINUTE / 60.0
        print(f"Producing to {KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC} "
              f"({ORDERS_PER_MINUTE}/min, speedup x{DEMO_SPEEDUP}, incidents={INCIDENTS})")
        self.publish_initial_inventory()
        next_order_at = time.time()
        try:
            while True:
                self.dispatch_due()
                if time.time() >= next_order_at:
                    self.emit_page_views()
                    self.create_order()
                    next_order_at = time.time() + random.expovariate(rate_per_sec)
                time.sleep(0.01)
        except KeyboardInterrupt:
            print("Stopping, flushing...")
        finally:
            self.producer.flush(10)


if __name__ == "__main__":
    Simulator().run()