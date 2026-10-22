"""Reconciliation rules: compare bookings with payments and classify mismatches.

The same `reconcile` function is used by the streaming job (on a bounded scope
every micro-batch) and by the nightly batch job (on everything), so the two
paths can never disagree about what counts as an exception.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

MISSING_PAYMENT = "MISSING_PAYMENT"
DUPLICATE_CHARGE = "DUPLICATE_CHARGE"
AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
REFUND_MISMATCH = "REFUND_MISMATCH"
ORPHAN_PAYMENT = "ORPHAN_PAYMENT"

EXCEPTION_TYPES = (
    MISSING_PAYMENT,
    DUPLICATE_CHARGE,
    AMOUNT_MISMATCH,
    CURRENCY_MISMATCH,
    REFUND_MISMATCH,
    ORPHAN_PAYMENT,
)

CAPTURED = "payment.captured"
REFUNDED = "refund.succeeded"

EXCEPTION_COLUMNS = [
    "exception_id",
    "booking_id",
    "exception_type",
    "expected_amount_cents",
    "actual_amount_cents",
    "amount_at_risk_cents",
    "booking_currency",
    "payment_currency",
    "payment_ids",
    "due_at",
]


def _interval_minutes(minutes: int) -> Column:
    return F.expr(f"INTERVAL {int(minutes)} MINUTES")


def _exception_id(booking_id: Column, exception_type: Column) -> Column:
    # One open exception per (booking, type). Deterministic so re-evaluation is idempotent.
    return F.sha2(F.concat_ws("|", booking_id, exception_type), 256)


def _first_per(df: DataFrame, key: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col("occurred_at").asc(), F.col("event_id").asc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def summarize_payments(payments: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Per-booking capture and refund totals.

    Captures are de-duplicated on payment_id: two deliveries of one capture are
    the same money, two different payment_ids are two charges.
    """
    captures = _first_per(payments.where(F.col("event_type") == CAPTURED), "payment_id")
    refunds = _first_per(payments.where(F.col("event_type") == REFUNDED), "payment_id")

    cap = captures.groupBy("booking_id").agg(
        F.countDistinct("payment_id").alias("n_captures"),
        F.sum("amount_cents").alias("captured_cents"),
        F.min("currency").alias("capture_currency"),
        F.countDistinct("currency").alias("n_capture_currencies"),
        F.concat_ws(",", F.array_sort(F.collect_set("payment_id"))).alias("capture_payment_ids"),
        F.min("occurred_at").alias("first_capture_at"),
        F.max("occurred_at").alias("last_capture_at"),
        F.max("received_at").alias("last_capture_received_at"),
    )
    ref = refunds.groupBy("booking_id").agg(
        F.sum("amount_cents").alias("refunded_cents"),
        F.max("occurred_at").alias("last_refund_at"),
    )
    return cap, ref


def reconcile(
    bookings: DataFrame,
    payments: DataFrame,
    known_booking_ids: DataFrame,
    as_of: datetime,
    sla_minutes: int,
) -> DataFrame:
    """Return the exceptions that are true as of `as_of` (an event-time watermark).

    bookings          current state per booking (silver.bookings columns)
    payments          payment events, one row per event_id
    known_booking_ids every booking_id that exists, used to spot orphan payments
    as_of             timezone-aware datetime; deadlines are compared to this
    sla_minutes       how long a payment or refund may lag before it is flagged
    """
    sla = _interval_minutes(sla_minutes)
    as_of_col = F.lit(as_of).cast("timestamp")
    cap, ref = summarize_payments(payments)

    b = (
        bookings.where(~F.coalesce(F.col("is_deleted"), F.lit(False)))
        .select(
            "booking_id",
            "status",
            "amount_cents",
            F.col("currency").alias("booking_currency"),
            "confirmed_at",
            "cancelled_at",
        )
        .join(cap, "booking_id", "left")
        .join(ref, "booking_id", "left")
        .withColumn("n_captures", F.coalesce("n_captures", F.lit(0)))
        .withColumn("captured_cents", F.coalesce("captured_cents", F.lit(0)))
        .withColumn("refunded_cents", F.coalesce("refunded_cents", F.lit(0)))
    )

    def shape(df: DataFrame, etype: str, expected: Column, actual: Column, risk: Column,
              pay_ccy: Column, ids: Column, due: Column) -> DataFrame:
        return df.select(
            _exception_id(F.col("booking_id"), F.lit(etype)).alias("exception_id"),
            "booking_id",
            F.lit(etype).alias("exception_type"),
            expected.cast("long").alias("expected_amount_cents"),
            actual.cast("long").alias("actual_amount_cents"),
            risk.cast("long").alias("amount_at_risk_cents"),
            F.col("booking_currency"),
            pay_ccy.alias("payment_currency"),
            ids.alias("payment_ids"),
            due.alias("due_at"),
        )

    ids = F.coalesce(F.col("capture_payment_ids"), F.lit(""))

    missing = shape(
        b.where(
            (F.col("status") == "CONFIRMED")
            & (F.col("n_captures") == 0)
            & ((F.col("confirmed_at") + sla) <= as_of_col)
        ),
        MISSING_PAYMENT,
        F.col("amount_cents"),
        F.lit(0),
        F.col("amount_cents"),
        F.lit(None).cast("string"),
        ids,
        F.col("confirmed_at") + sla,
    )

    duplicate = shape(
        b.where(F.col("n_captures") > 1),
        DUPLICATE_CHARGE,
        F.col("amount_cents"),
        F.col("captured_cents"),
        F.greatest(F.col("captured_cents") - F.col("amount_cents"), F.lit(0)),
        F.col("capture_currency"),
        ids,
        F.col("last_capture_at"),
    )

    single = b.where(F.col("n_captures") == 1)
    currency = shape(
        single.where(F.col("capture_currency") != F.col("booking_currency")),
        CURRENCY_MISMATCH,
        F.col("amount_cents"),
        F.col("captured_cents"),
        F.col("amount_cents"),
        F.col("capture_currency"),
        ids,
        F.col("last_capture_at"),
    )
    amount = shape(
        single.where(
            (F.col("capture_currency") == F.col("booking_currency"))
            & (F.col("captured_cents") != F.col("amount_cents"))
        ),
        AMOUNT_MISMATCH,
        F.col("amount_cents"),
        F.col("captured_cents"),
        F.abs(F.col("captured_cents") - F.col("amount_cents")),
        F.col("capture_currency"),
        ids,
        F.col("last_capture_at"),
    )

    refund = shape(
        b.where(
            (F.col("status") == "CANCELLED")
            & (F.col("captured_cents") > 0)
            & (F.col("refunded_cents") != F.col("captured_cents"))
            & ((F.col("cancelled_at") + sla) <= as_of_col)
        ),
        REFUND_MISMATCH,
        F.col("captured_cents"),
        F.col("refunded_cents"),
        F.abs(F.col("captured_cents") - F.col("refunded_cents")),
        F.col("capture_currency"),
        ids,
        F.col("cancelled_at") + sla,
    )

    orphans = (
        cap.join(known_booking_ids.select("booking_id").distinct(), "booking_id", "left_anti")
        .where((F.col("first_capture_at") + sla) <= as_of_col)
        .withColumn("booking_currency", F.lit(None).cast("string"))
    )
    orphan = shape(
        orphans,
        ORPHAN_PAYMENT,
        F.lit(0),
        F.col("captured_cents"),
        F.col("captured_cents"),
        F.col("capture_currency"),
        F.col("capture_payment_ids"),
        F.col("first_capture_at") + sla,
    )

    result = missing
    for part in (duplicate, currency, amount, refund, orphan):
        result = result.unionByName(part)
    return result


def plan_exception_changes(
    current: DataFrame,
    existing: DataFrame,
    scope_ids: DataFrame,
    evaluated_at: datetime,
    detected_by: str,
) -> DataFrame:
    """Diff freshly computed exceptions against the stored ones.

    current    output of `reconcile` for the evaluated scope
    existing   rows already in gold.recon_exceptions (any status)
    scope_ids  booking_ids that were evaluated; only these may be auto-resolved

    Returns one row per change with a `change` column (UPSERT or RESOLVE) and an
    `alert` flag for exceptions that are new or re-opened.
    """
    ts = F.lit(evaluated_at).cast("timestamp")
    prior = existing.select(
        "exception_id", F.col("status").alias("prior_status")
    )

    upserts = (
        current.join(prior, "exception_id", "left")
        .withColumn("change", F.lit("UPSERT"))
        .withColumn(
            "alert",
            F.col("prior_status").isNull() | (F.col("prior_status") == "RESOLVED"),
        )
        .drop("prior_status")
    )

    in_scope_open = existing.where(F.col("status") == "OPEN").join(
        scope_ids.select("booking_id").distinct(), "booking_id", "left_semi"
    )
    resolves = (
        in_scope_open.join(current.select("exception_id"), "exception_id", "left_anti")
        .select(*EXCEPTION_COLUMNS)
        .withColumn("change", F.lit("RESOLVE"))
        .withColumn("alert", F.lit(False))
    )

    return (
        upserts.unionByName(resolves)
        .withColumn("evaluated_at", ts)
        .withColumn("detected_by", F.lit(detected_by))
    )
