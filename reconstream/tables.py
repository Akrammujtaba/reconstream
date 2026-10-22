"""Iceberg table definitions.

Silver and gold tables use merge-on-read: streaming MERGEs write small delete
files instead of rewriting data files every 10 seconds, and the nightly
maintenance job compacts them.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from reconstream.config import (
    BRONZE_EVENTS,
    GOLD_EXCEPTIONS,
    SILVER_BOOKINGS,
    SILVER_PAYMENTS,
)

MOR_PROPS = """
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.merge.isolation-level' = 'snapshot',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '50'
"""

DDL = {
    BRONZE_EVENTS: f"""
        CREATE TABLE IF NOT EXISTS {{t}} (
            topic STRING, kafka_partition INT, kafka_offset BIGINT, key STRING,
            value STRING, kafka_ts TIMESTAMP, batch_id BIGINT, ingested_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (topic, days(kafka_ts))
        TBLPROPERTIES ({MOR_PROPS})
    """,
    SILVER_BOOKINGS: f"""
        CREATE TABLE IF NOT EXISTS {{t}} (
            booking_id STRING, guest_id STRING, listing_id STRING, status STRING,
            amount_cents BIGINT, currency STRING, created_at TIMESTAMP,
            confirmed_at TIMESTAMP, cancelled_at TIMESTAMP, updated_at TIMESTAMP,
            is_deleted BOOLEAN, source_lsn BIGINT, cdc_op STRING, ingested_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(created_at))
        TBLPROPERTIES ({MOR_PROPS})
    """,
    SILVER_PAYMENTS: f"""
        CREATE TABLE IF NOT EXISTS {{t}} (
            event_id STRING, event_type STRING, payment_id STRING, booking_id STRING,
            amount_cents BIGINT, currency STRING, occurred_at TIMESTAMP,
            received_at TIMESTAMP, kafka_ts TIMESTAMP, ingested_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(occurred_at))
        TBLPROPERTIES ({MOR_PROPS})
    """,
    GOLD_EXCEPTIONS: f"""
        CREATE TABLE IF NOT EXISTS {{t}} (
            exception_id STRING, booking_id STRING, exception_type STRING, status STRING,
            expected_amount_cents BIGINT, actual_amount_cents BIGINT,
            amount_at_risk_cents BIGINT, booking_currency STRING, payment_currency STRING,
            payment_ids STRING, due_at TIMESTAMP, first_detected_at TIMESTAMP,
            last_seen_at TIMESTAMP, last_evaluated_at TIMESTAMP, resolved_at TIMESTAMP,
            detected_by STRING, reopen_count INT
        ) USING iceberg
        TBLPROPERTIES ({MOR_PROPS})
    """,
}


def ensure_tables(spark: SparkSession, catalog: str) -> None:
    for ns in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}")
    for table, ddl in DDL.items():
        spark.sql(ddl.format(t=f"{catalog}.{table}"))
