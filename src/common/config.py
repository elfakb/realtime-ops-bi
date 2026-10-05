"""Shared configuration for host-side scripts (generator, alert checker)."""
import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce.events")

PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "opsdb"),
    "user": os.getenv("POSTGRES_USER", "ops"),
    "password": os.getenv("POSTGRES_PASSWORD", "ops_password"),
}

ORDERS_PER_MINUTE = float(os.getenv("ORDERS_PER_MINUTE", "60"))
PAGEVIEWS_PER_ORDER = int(os.getenv("PAGEVIEWS_PER_ORDER", "10"))
INCIDENTS = os.getenv("INCIDENTS", "true").lower() == "true"
DEMO_SPEEDUP = float(os.getenv("DEMO_SPEEDUP", "1"))

ALERT_INTERVAL_SECONDS = int(os.getenv("ALERT_INTERVAL_SECONDS", "30"))
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")