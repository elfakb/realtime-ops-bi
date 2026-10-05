"""
Spark Structured Streaming job:
Kafka -> parse + validate -> PostgreSQL (events_ingest) / dead letter queue.

Idempotency is enforced in PostgreSQL (ON CONFLICT DO NOTHING on event_id),
so Spark's at-least-once delivery becomes effectively exactly-once.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce.events")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/events")

JDBC_URL = "jdbc:postgresql://{h}:{p}/{d}".format(
    h=os.getenv("POSTGRES_HOST", "postgres"),
    p=os.getenv("POSTGRES_PORT", "5432"),
    d=os.getenv("POSTGRES_DB", "opsdb"),
)
JDBC_PROPS = {
    "user": os.getenv("POSTGRES_USER", "ops"),
    "password": os.getenv("POSTGRES_PASSWORD", "ops_password"),
    "driver": "org.postgresql.Driver",
}

VALID_TYPES = [
    "ORDER_CREATED",
    "PAYMENT_COMPLETED",
    "ORDER_SHIPPED",
    "ORDER_DELIVERED",
    "ORDER_CANCELLED",
    "INVENTORY_UPDATED",
    "PAGE_VIEW",
]

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),   # ISO-8601 string, cast below
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("amount", DecimalType(12, 2)),
    StructField("sku", StringType()),
    StructField("quantity", IntegerType()),
    StructField("stock_level", IntegerType()),
    StructField("reorder_point", IntegerType()),
    StructField("city", StringType()),
    StructField("cancel_reason", StringType()),
])

EVENT_COLS = [f.name for f in EVENT_SCHEMA.fields]


def build_stream(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    text = raw.select(F.col("value").cast("string").alias("raw"))
    parsed = text.select("raw", F.from_json("raw", EVENT_SCHEMA).alias("e")).select("raw", "e.*")
    return parsed.withColumn("event_time", F.to_timestamp("event_time"))


def is_valid_expr():
    base = (
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("event_type").isin(*VALID_TYPES)
    )
    needs_order = F.when(
        F.col("event_type").startswith("ORDER_") | (F.col("event_type") == "PAYMENT_COMPLETED"),
        F.col("order_id").isNotNull(),
    ).otherwise(F.lit(True))
    positive_amount = F.coalesce(F.col("amount") >= 0, F.lit(True))
    return base & needs_order & positive_amount


def write_batch(batch_df, batch_id: int) -> None:
    batch_df.persist()
    valid = is_valid_expr()

    good = batch_df.filter(valid).select(*EVENT_COLS)
    good.write.jdbc(JDBC_URL, "events_ingest", mode="append", properties=JDBC_PROPS)

    bad = batch_df.filter(~valid).select(
        F.col("raw").alias("payload"),
        F.lit(batch_id).cast("long").alias("batch_id"),
    )
    bad.write.jdbc(JDBC_URL, "events_dead_letter", mode="append", properties=JDBC_PROPS)

    batch_df.unpersist()


def main() -> None:
    spark = (
        SparkSession.builder.appName("realtime-ops-bi")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    query = (
        build_stream(spark)
        .writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="5 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()