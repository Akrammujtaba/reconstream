"""Nightly full reconciliation: the backstop for anything the stream could not judge.

The stream only re-checks bookings inside its horizon and only advances when
events arrive. This job re-runs the same rules over a whole date range with a
wall-clock "now", so nothing slips through on quiet nights or after outages.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from pyspark.sql import functions as F

from reconstream.config import Settings
from reconstream.reconcile import plan_exception_changes, reconcile


def run_batch(store, settings: Settings, start: date | None = None, end: date | None = None,
              now: datetime | None = None) -> dict:
    """Reconcile bookings created, or paid for, in [start, end). No dates = everything."""
    now = now or datetime.now(timezone.utc)
    as_of = now - timedelta(seconds=settings.allowed_lateness_seconds)
    bookings = store.bookings()
    payments = store.payments()

    if start is None and end is None:
        b_keys, p_keys = bookings, payments
    else:
        lo = F.lit(datetime.combine(start or date.min, datetime.min.time(), timezone.utc))
        hi = F.lit(datetime.combine(end or date.max, datetime.min.time(), timezone.utc))
        b_keys = bookings.where((F.col("created_at") >= lo) & (F.col("created_at") < hi))
        p_keys = payments.where((F.col("occurred_at") >= lo) & (F.col("occurred_at") < hi))

    scope = (
        b_keys.select("booking_id").unionByName(p_keys.select("booking_id")).distinct().persist()
    )
    try:
        current = reconcile(
            bookings.join(scope, "booking_id", "left_semi"),
            payments.join(scope, "booking_id", "left_semi"),
            bookings.select("booking_id").join(scope, "booking_id", "left_semi"),
            as_of=as_of,
            sla_minutes=settings.sla_minutes,
            sla_minutes_by_currency=settings.sla_minutes_by_currency,
            refund_tolerance_cents=settings.refund_tolerance_cents,
        )
        existing = store.exceptions().join(scope, "booking_id", "left_semi")
        plan = plan_exception_changes(current, existing, scope, now, detected_by="batch").persist()
        try:
            counts = {(r["change"], r["alert"]): r["count"]
                      for r in plan.groupBy("change", "alert").count().collect()}
            if counts:
                store.merge_exceptions(plan)
                store.publish_alerts(plan.where(F.col("alert")))
        finally:
            plan.unpersist()
        evaluated = scope.count()
    finally:
        scope.unpersist()

    return {
        "bookings_evaluated": evaluated,
        "new_exceptions": counts.get(("UPSERT", True), 0),
        "still_open": counts.get(("UPSERT", False), 0),
        "resolved": counts.get(("RESOLVE", False), 0),
    }
