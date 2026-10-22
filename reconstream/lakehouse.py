"""Idempotent writes into the Iceberg tables.

Every write is safe to repeat: if Spark replays a micro-batch after a crash,
the MERGE keys (LSN for bookings, event_id for payments, exception_id for
exceptions) stop duplicates from landing in silver or gold.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from reconstream.reconcile import EXCEPTION_COLUMNS

BOOKING_COLUMNS = [
    "booking_id", "guest_id", "listing_id", "status", "amount_cents", "currency",
    "created_at", "confirmed_at", "cancelled_at", "updated_at", "is_deleted",
    "source_lsn", "cdc_op", "ingested_at",
]
PAYMENT_COLUMNS = [
    "event_id", "event_type", "payment_id", "booking_id", "amount_cents", "currency",
    "occurred_at", "received_at", "kafka_ts", "ingested_at",
]


def append_bronze(raw: DataFrame, table: str, batch_id: int) -> None:
    """Append raw Kafka records as a replayable log.

    Bronze is at-least-once: a replayed batch can repeat rows here, which is
    fine because nothing downstream reads bronze without de-duplicating.
    """
    (
        raw.select(
            "topic",
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            F.col("timestamp").alias("kafka_ts"),
            F.lit(batch_id).cast("long").alias("batch_id"),
            F.current_timestamp().alias("ingested_at"),
        )
        .writeTo(table)
        .append()
    )


def _ts(value: datetime) -> str:
    return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def _range_clause(column: str, bounds: tuple | None) -> str:
    """Extra ON-clause predicate that lets Iceberg skip files outside the batch's range.

    Only used on columns that never change for a key (a booking's created_at, an
    event's occurred_at), so it cannot hide a real match.
    """
    if not bounds or bounds[0] is None or bounds[1] is None:
        return ""
    lo, hi = bounds
    return f" AND t.{column} BETWEEN {_ts(lo)} AND {_ts(hi)}"


def merge_bookings(latest: DataFrame, table: str, created_range: tuple | None = None) -> None:
    """Upsert the newest change per booking; an older LSN never overwrites a newer one."""
    view = "_src_bookings"
    latest.withColumn("ingested_at", F.current_timestamp()).select(
        *BOOKING_COLUMNS
    ).createOrReplaceTempView(view)
    latest.sparkSession.sql(
        f"""
        MERGE INTO {table} t
        USING {view} s
        ON t.booking_id = s.booking_id{_range_clause("created_at", created_range)}
        WHEN MATCHED AND (t.source_lsn IS NULL OR s.source_lsn > t.source_lsn) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def merge_payments(events: DataFrame, table: str, occurred_range: tuple | None = None) -> None:
    """Insert-only on event_id, so redelivered webhooks are ignored."""
    view = "_src_payments"
    events.withColumn("ingested_at", F.current_timestamp()).select(
        *PAYMENT_COLUMNS
    ).createOrReplaceTempView(view)
    events.sparkSession.sql(
        f"""
        MERGE INTO {table} t
        USING {view} s
        ON t.event_id = s.event_id{_range_clause("occurred_at", occurred_range)}
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def merge_exceptions(changes: DataFrame, table: str) -> None:
    """Apply the plan from reconcile.plan_exception_changes."""
    view = "_src_exception_changes"
    changes.select(
        *EXCEPTION_COLUMNS, "change", "evaluated_at", "detected_by"
    ).createOrReplaceTempView(view)
    changes.sparkSession.sql(
        f"""
        MERGE INTO {table} t
        USING {view} s
        ON t.exception_id = s.exception_id
        WHEN MATCHED AND s.change = 'RESOLVE' AND t.status = 'OPEN' THEN UPDATE SET
            t.status = 'RESOLVED',
            t.resolved_at = s.evaluated_at,
            t.last_evaluated_at = s.evaluated_at
        WHEN MATCHED AND s.change = 'UPSERT' THEN UPDATE SET
            t.reopen_count = t.reopen_count + CASE WHEN t.status = 'RESOLVED' THEN 1 ELSE 0 END,
            t.status = 'OPEN',
            t.resolved_at = NULL,
            t.expected_amount_cents = s.expected_amount_cents,
            t.actual_amount_cents = s.actual_amount_cents,
            t.amount_at_risk_cents = s.amount_at_risk_cents,
            t.payment_currency = s.payment_currency,
            t.payment_ids = s.payment_ids,
            t.due_at = s.due_at,
            t.last_seen_at = s.evaluated_at,
            t.last_evaluated_at = s.evaluated_at
        WHEN NOT MATCHED AND s.change = 'UPSERT' THEN INSERT (
            exception_id, booking_id, exception_type, status, expected_amount_cents,
            actual_amount_cents, amount_at_risk_cents, booking_currency, payment_currency,
            payment_ids, due_at, first_detected_at, last_seen_at, last_evaluated_at,
            resolved_at, detected_by, reopen_count
        ) VALUES (
            s.exception_id, s.booking_id, s.exception_type, 'OPEN', s.expected_amount_cents,
            s.actual_amount_cents, s.amount_at_risk_cents, s.booking_currency,
            s.payment_currency, s.payment_ids, s.due_at, s.evaluated_at, s.evaluated_at,
            s.evaluated_at, NULL, s.detected_by, 0
        )
        """
    )
