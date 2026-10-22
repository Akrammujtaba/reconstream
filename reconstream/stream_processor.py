"""Per-micro-batch logic for the streaming reconciliation job.

Each micro-batch:
  1. lands raw records in bronze (replay log)
  2. upserts bookings (CDC) and payments (webhooks) into silver, idempotently
  3. advances an event-time watermark: newest event seen minus allowed lateness
  4. re-reconciles every booking touched within the horizon, using the watermark
     as "now" so timeouts are judged on event time, not on when Spark ran
  5. merges the resulting changes into gold, alerts on new or re-opened ones

Why foreachBatch + MERGE instead of a stream-stream join: exceptions must be
able to change state later (a late payment resolves a MISSING_PAYMENT), and a
streaming join can only emit a result once.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from reconstream.config import Settings
from reconstream.observability import StreamMetrics, post_slack_summary
from reconstream.parsing import latest_booking_per_key, parse_booking_changes, parse_payment_events
from reconstream.reconcile import plan_exception_changes, reconcile

log = logging.getLogger("reconstream.stream")


def as_utc(value: datetime | None) -> datetime | None:
    """Spark hands back naive datetimes in the session/OS zone, which is UTC here."""
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


class StreamProcessor:
    def __init__(self, store, settings: Settings, metrics: StreamMetrics | None = None,
                 clock=lambda: datetime.now(timezone.utc)):
        self.store = store
        self.settings = settings
        self.metrics = metrics or StreamMetrics(settings.pushgateway)
        self.clock = clock
        self.lateness = timedelta(seconds=settings.allowed_lateness_seconds)
        self.watermark: datetime | None = self._initial_watermark()

    def _initial_watermark(self) -> datetime | None:
        """Resume the watermark from silver so a restart doesn't re-flag old timeouts."""
        b = self.store.bookings().agg(F.max("updated_at").alias("m")).first()
        p = self.store.payments().agg(F.max("occurred_at").alias("m")).first()
        latest = [as_utc(x.m) for x in (b, p) if x is not None and x.m is not None]
        return max(latest) - self.lateness if latest else None

    # Spark calls this for every micro-batch.
    def process(self, batch: DataFrame, batch_id: int) -> None:
        started = time.time()
        batch = batch.persist()
        try:
            stats = self._process(batch, batch_id)
        finally:
            batch.unpersist()
        stats["duration_s"] = time.time() - started
        if self.watermark is not None:
            stats["watermark_lag_s"] = (self.clock() - self.watermark).total_seconds()
        self.metrics.push(stats, self._open_by_type())
        log.info("batch %s: %s", batch_id, {k: round(v, 2) for k, v in stats.items()})

    def _process(self, batch: DataFrame, batch_id: int) -> dict:
        s = self.settings
        stats: dict = {"input_rows": batch.count()}
        if stats["input_rows"] == 0:
            return stats

        self.store.append_bronze(batch, batch_id)

        changes = parse_booking_changes(batch.where(F.col("topic") == s.bookings_topic))
        latest = latest_booking_per_key(changes).persist()
        payments = parse_payment_events(batch.where(F.col("topic") == s.payments_topic)).persist()
        try:
            stats["bookings_events"] = changes.count()
            stats["payment_events"] = payments.count()
            stats["duplicate_deliveries"] = self._count_redeliveries(batch, payments)

            if stats["bookings_events"]:
                self.store.merge_bookings(latest)
            if stats["payment_events"]:
                self.store.merge_payments(payments)

            self._advance_watermark(latest, payments)
        finally:
            latest.unpersist()
            payments.unpersist()

        if self.watermark is not None:
            stats.update(self._reconcile_recent())
        return stats

    def _count_redeliveries(self, batch: DataFrame, new_events: DataFrame) -> int:
        """Webhook deliveries that repeat an event_id we already had (in this batch or earlier)."""
        delivered = batch.where(F.col("topic") == self.settings.payments_topic).count()
        if not delivered:
            return 0
        bounds = new_events.agg(F.min("occurred_at").alias("lo")).first()
        known = self.store.payments().select("event_id")
        if bounds and bounds.lo is not None:
            known = self.store.payments().where(
                F.col("occurred_at") >= F.lit(bounds.lo) - F.expr("INTERVAL 1 DAY")
            ).select("event_id")
        unseen = new_events.join(known, "event_id", "left_anti").count()
        return max(delivered - unseen, 0)

    def _advance_watermark(self, latest: DataFrame, payments: DataFrame) -> None:
        maxima = [
            latest.agg(F.max("updated_at").alias("m")).first(),
            payments.agg(F.max("occurred_at").alias("m")).first(),
        ]
        seen = [as_utc(r.m) for r in maxima if r is not None and r.m is not None]
        if not seen:
            return
        candidate = max(seen) - self.lateness
        if self.watermark is None or candidate > self.watermark:
            self.watermark = candidate

    def _reconcile_recent(self) -> dict:
        s = self.settings
        since = F.lit(self.watermark - timedelta(minutes=s.horizon_minutes))
        bookings = self.store.bookings()
        payments = self.store.payments()

        scope = (
            bookings.where(F.col("updated_at") >= since).select("booking_id")
            .unionByName(payments.where(F.col("occurred_at") >= since).select("booking_id"))
            .distinct()
            .persist()
        )
        try:
            current = reconcile(
                bookings.join(scope, "booking_id", "left_semi"),
                payments.join(scope, "booking_id", "left_semi"),
                bookings.select("booking_id").join(scope, "booking_id", "left_semi"),
                as_of=self.watermark,
                sla_minutes=s.sla_minutes,
            )
            existing = self.store.exceptions().join(scope, "booking_id", "left_semi")
            plan = plan_exception_changes(
                current, existing, scope, evaluated_at=self.clock(), detected_by="stream"
            ).persist()
            try:
                plan_stats = plan.groupBy("change", "alert").count().collect()
                if not plan_stats:
                    return {"new_exceptions": 0, "resolved_exceptions": 0}
                self.store.merge_exceptions(plan)
                alerts = plan.where(F.col("alert"))
                self.store.publish_alerts(alerts)
                if s.slack_webhook_url:
                    post_slack_summary(
                        s.slack_webhook_url, [r.asDict() for r in alerts.limit(50).collect()]
                    )
            finally:
                plan.unpersist()
        finally:
            scope.unpersist()

        new = sum(r["count"] for r in plan_stats if r["change"] == "UPSERT" and r["alert"])
        resolved = sum(r["count"] for r in plan_stats if r["change"] == "RESOLVE")
        return {"new_exceptions": new, "resolved_exceptions": resolved}

    def _open_by_type(self) -> dict[str, tuple[int, int]]:
        rows = (
            self.store.exceptions()
            .where(F.col("status") == "OPEN")
            .groupBy("exception_type")
            .agg(F.count("*").alias("n"), F.sum("amount_at_risk_cents").alias("risk"))
            .collect()
        )
        return {r.exception_type: (r.n, r.risk or 0) for r in rows}
