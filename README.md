# Real-Time Operations BI

End-to-end real-time operations monitoring for an e-commerce company:
**Kafka -> Spark Structured Streaming -> PostgreSQL -> Power BI**, with threshold alerting.

## Scenario
An e-commerce company wants to see what is happening *right now*: orders, payments,
deliveries, inventory, and website traffic - and be alerted when something goes wrong
(e.g. cancellation rate above 5 %, stock below reorder point).

## Architecture

```
 Applications (simulated)
        |
        v
     Kafka  (topic: ecommerce.events, 3 partitions, KRaft)
        |
        v
 Spark Structured Streaming
   - JSON parsing, validation
   - invalid messages -> events_dead_letter
        |
        v
 PostgreSQL
   events_ingest --(trigger, ON CONFLICT DO NOTHING)--> events (immutable log)
   events --(trigger, upserts)--> orders / inventory (current state)
   SQL views  -> KPIs, charts, SLA, alerts
        |                         \
        v                          v
    Power BI (DirectQuery)     Alert checker (Python) -> console / Slack
```

## Event types
`ORDER_CREATED`, `PAYMENT_COMPLETED`, `ORDER_SHIPPED`, `ORDER_DELIVERED`,
`ORDER_CANCELLED`, `INVENTORY_UPDATED`, `PAGE_VIEW`

Example event:
```json
{
  "event_id": "b4c6...",
  "event_type": "ORDER_CREATED",
  "event_time": "2026-10-05T10:15:30+00:00",
  "order_id": 10294,
  "customer_id": 4821,
  "sku": "SKU-0012",
  "quantity": 1,
  "amount": 1499.0,
  "city": "Istanbul"
}
```

## Dashboard: Real-Time Operations Center
KPIs: Orders Today, Revenue Today, Pending Orders, Cancellation Rate, Avg Processing Time, SLA Breaches.
Charts: orders/minute, revenue/hour, cancellation rate, processing time (avg + p95),
delivery SLA, inventory alerts, order status breakdown.
See [`powerbi/README.md`](powerbi/README.md) for the full guide and DAX measures.

## Alerts
Rules live in the `alert_rules` table (no code changes needed to tune them):

| Alert | Default rule |
|-------|--------------|
| Cancellation rate | > 5 % over the last 15 min (min. 20 orders) |
| SLA breaches | > 20 today |
| Inventory | stock <= reorder point (per SKU) |

The alert checker opens an alert once, resolves it when the condition clears, and
optionally posts to Slack (`SLACK_WEBHOOK_URL`).

## Key engineering decisions
- **Idempotent ingestion**: duplicates are dropped in PostgreSQL via `ON CONFLICT DO NOTHING`
  on `event_id`, so Spark's at-least-once delivery is safe.
- **Out-of-order tolerance**: the `orders` table stores only timestamps; status is derived in a view.
- **Dead letter queue**: malformed or invalid events are kept for inspection, never dropped silently.
- **BI isolation**: Power BI reads only from views, so the schema can evolve without breaking reports.
- **Realistic simulation**: slow orders (SLA breaches) and periodic cancellation spikes (to trigger alerts).

## Project structure
```
src/producer/       event generator (Kafka producer)
src/streaming/      Spark Structured Streaming job
src/alerts/         threshold alert checker
sql/                schema, views, alert rules
powerbi/            dashboard guide
tests/              unit tests
```

## Quick start
Requirements: Docker, Python 3.10+

```bash
cp .env.example .env
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

docker compose up -d                  # Kafka, PostgreSQL, Spark
python -m src.producer.event_generator   # terminal 2
python -m src.alerts.alert_checker       # terminal 3
```

Verify:
```bash
docker compose logs -f spark
docker compose exec postgres psql -U ops -d opsdb -c "SELECT * FROM v_kpi_today;"
```

Then connect Power BI (see `powerbi/README.md`).

## Tech stack
Apache Kafka (KRaft), Apache Spark 3.5 (Structured Streaming, PySpark), PostgreSQL 16,
Python, SQL, Docker Compose, Power BI (DirectQuery, DAX).
