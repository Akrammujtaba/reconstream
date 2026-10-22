"""Turn raw Kafka records into typed, de-duplicated rows.

Every function here takes and returns plain DataFrames so it can be unit tested
without Kafka or Iceberg.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from reconstream.schemas import DEBEZIUM_BOOKING_ENVELOPE, PAYMENT_EVENT

PAYMENT_TYPES = ("payment.captured", "payment.failed", "refund.succeeded")


def parse_booking_changes(raw: DataFrame) -> DataFrame:
    """Debezium envelope -> one row per change.

    Deletes keep the `before` image and are flagged with is_deleted, so the
    silver table can soft-delete instead of losing history.
    """
    env = F.from_json(F.col("value").cast("string"), DEBEZIUM_BOOKING_ENVELOPE)
    parsed = raw.select(env.alias("e"), F.col("timestamp").alias("kafka_ts"))
    row = F.coalesce(F.col("e.after"), F.col("e.before"))

    def ts(field: str):
        return F.to_timestamp(row.getField(field))

    return parsed.where(F.col("e").isNotNull() & row.isNotNull()).select(
        row.getField("booking_id").alias("booking_id"),
        row.getField("guest_id").alias("guest_id"),
        row.getField("listing_id").alias("listing_id"),
        row.getField("status").alias("status"),
        row.getField("amount_cents").alias("amount_cents"),
        row.getField("currency").alias("currency"),
        ts("created_at").alias("created_at"),
        ts("confirmed_at").alias("confirmed_at"),
        ts("cancelled_at").alias("cancelled_at"),
        ts("updated_at").alias("updated_at"),
        (F.col("e.op") == F.lit("d")).alias("is_deleted"),
        F.col("e.source.lsn").alias("source_lsn"),
        F.col("e.op").alias("cdc_op"),
        F.col("kafka_ts"),
    )


def latest_booking_per_key(changes: DataFrame) -> DataFrame:
    """Keep only the newest change per booking within a micro-batch.

    Ordering uses the Postgres LSN, which is strictly increasing, rather than
    updated_at, which can tie or go backwards if application clocks drift.
    """
    w = Window.partitionBy("booking_id").orderBy(
        F.col("source_lsn").desc_nulls_last(), F.col("kafka_ts").desc()
    )
    return changes.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def parse_payment_events(raw: DataFrame) -> DataFrame:
    """Webhook JSON -> typed payment events, de-duplicated on event_id.

    The payment provider can deliver the same event more than once. Duplicates
    share an event_id, so they collapse here and again on MERGE into silver.
    """
    evt = F.from_json(F.col("value").cast("string"), PAYMENT_EVENT)
    parsed = raw.select(evt.alias("p"), F.col("timestamp").alias("kafka_ts")).select(
        F.col("p.event_id").alias("event_id"),
        F.col("p.type").alias("event_type"),
        F.col("p.payment_id").alias("payment_id"),
        F.col("p.booking_id").alias("booking_id"),
        F.col("p.amount_cents").alias("amount_cents"),
        F.col("p.currency").alias("currency"),
        F.to_timestamp("p.occurred_at").alias("occurred_at"),
        F.to_timestamp("p.received_at").alias("received_at"),
        F.col("kafka_ts"),
    )
    valid = (
        F.col("event_id").isNotNull()
        & F.col("payment_id").isNotNull()
        & F.col("booking_id").isNotNull()
        & F.col("occurred_at").isNotNull()
        & F.col("event_type").isin(*PAYMENT_TYPES)
    )
    w = Window.partitionBy("event_id").orderBy(F.col("kafka_ts").asc())
    return (
        parsed.where(valid)
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
    )
