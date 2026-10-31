"""Per-micro-batch logic for the streaming reconciliation job.

Each micro-batch:
  1. lands raw records in bronze (replay log)
  2. upserts bookings (CDC) and payments (webhooks) into silver, idempotently
  3. advances an event-time watermark: newest event seen minus allowed lateness
  4. re-reconciles only the bookings whose answer can have changed:
       - bookings or payments that arrived in this batch, and
       - bookings whose SLA deadline fell between the previous and the new watermark
     so the work per batch tracks batch size, not table size
  5. merges the resulting changes into gold, alerts on new or re-opened ones
  6. records how many data and delete files the MERGEs added, and compacts
     once delete files pile up on any table (or every N batches as a fallback),
     so reads don't slow down as the tables grow

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
from reconstream.reconcile import CAPTURED, plan_exception_changes, reconcile, sla_interval

log = logging.getLogger("reconstream.stream")


def compaction_reason(batches_since: int, every: int, open_delete_files: int,
                      threshold: int) -> str | None:
    """Why to compact now, or None.

    Delete files are the cost that matters: every merge-on-read update adds one,
    and every later read has to open and apply all of them. Compacting on that
    count does the work when it is needed instead of on a fixed schedule. The
    batch count stays as a fallback, because data files also grow on tables that
    never get deletes, and because the delete count may be unavailable.
    """
    if threshold and open_delete_files >= threshold:
        return "delete_files"
    if every and batches_since >= every:
        return "batch_count"
    return None


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
        # Shortest and longest SLA across all currencies; they bound the deadline window.
        all_slas = [settings.sla_minutes, *settings.sla_minutes_by_currency.values()]
        self.min_sla = timedelta(minutes=min(all_slas))
        self.max_sla = timedelta(minutes=max(all_slas))
        self.watermark: datetime | None = self._initial_watermark()
        # Watermark at the last reconciliation. None means "catch up over the horizon".
        self.evaluated_through: datetime | None = None
        self.batches_since_compaction = 0
        # Newest snapshot id seen per table, so a snapshot's files are counted once.
        self.seen_snapshots: dict[str, int] = {}
        self.open_delete_files = 0
        # Set after a failed delete-file-triggered compaction: the count would still be
        # over the threshold, and retrying every batch would stall the stream.
        self.delete_trigger_paused = False
        self._record_file_growth({})

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
        if stats.get("input_rows", 0):
            self._record_file_growth(stats)
        self._maybe_compact(stats)
        stats["duration_s"] = time.time() - started
        if self.watermark is not None:
            stats["watermark_lag_s"] = (self.clock() - self.watermark).total_seconds()
        self.metrics.push(stats, self._open_by_type())
        self.last_stats = stats
        log.info("batch %s: %s", batch_id, {k: round(v, 2) for k, v in stats.items()})

    def _process(self, batch: DataFrame, batch_id: int) -> dict:
        """Kept to as few Spark actions as possible: each one costs a job launch,
        and with small batches that fixed cost dominates."""
        s = self.settings
        per_topic = {r["topic"]: r["count"] for r in batch.groupBy("topic").count().collect()}
        stats: dict = {"input_rows": sum(per_topic.values())}
        if stats["input_rows"] == 0:
            return stats

        self.store.append_bronze(batch, batch_id)

        changes = parse_booking_changes(batch.where(F.col("topic") == s.bookings_topic))
        latest = latest_booking_per_key(changes).persist()
        payments = parse_payment_events(batch.where(F.col("topic") == s.payments_topic)).persist()
        touched = (
            latest.select("booking_id").unionByName(payments.select("booking_id"))
            .distinct()
            .persist()
        )
        try:
            # One pass each for counts, MERGE pruning bounds, and the watermark.
            b = latest.agg(F.count("*").alias("n"), F.min("created_at").alias("lo"),
                           F.max("created_at").alias("hi"),
                           F.max("updated_at").alias("newest")).first()
            p = payments.agg(F.count("*").alias("n"), F.min("occurred_at").alias("lo"),
                             F.max("occurred_at").alias("hi")).first()
            stats["bookings_events"] = per_topic.get(s.bookings_topic, 0)
            stats["payment_events"] = p.n
            stats["duplicate_deliveries"] = self._count_redeliveries(
                per_topic.get(s.payments_topic, 0), payments, p
            )

            if b.n:
                self.store.merge_bookings(latest, created_range=(b.lo, b.hi))
            if p.n:
                self.store.merge_payments(payments, occurred_range=(p.lo, p.hi))

            self._advance_watermark([b.newest, p.hi])
            if self.watermark is not None:
                stats.update(self._reconcile(touched))
        finally:
            latest.unpersist()
            payments.unpersist()
            touched.unpersist()
        return stats

    def _count_redeliveries(self, delivered: int, new_events: DataFrame, bounds) -> int:
        """Webhook deliveries that repeat an event_id we already had (in this batch or earlier).

        A redelivery carries the same occurred_at as the original, so silver only
        needs to be read for this batch's occurred_at range, which Iceberg prunes
        to a handful of files using column statistics.
        """
        if not delivered or bounds.n == 0:
            return delivered
        known = self.store.payments().where(
            F.col("occurred_at").between(F.lit(bounds.lo), F.lit(bounds.hi))
        ).select("event_id")
        unseen = new_events.join(known, "event_id", "left_anti").count()
        return max(delivered - unseen, 0)

    def _advance_watermark(self, newest_event_times: list) -> None:
        seen = [as_utc(t) for t in newest_event_times if t is not None]
        if not seen:
            return
        candidate = max(seen) - self.lateness
        if self.watermark is None or candidate > self.watermark:
            self.watermark = candidate

    def _deadline_scope(self, bookings: DataFrame, payments: DataFrame) -> DataFrame:
        """Bookings whose SLA deadline passed since the last evaluation.

        A deadline is (confirmed_at | cancelled_at | first capture) + SLA, where the
        SLA depends on the row's currency. It "passes" when the watermark moves beyond
        it, so we want deadlines in (evaluated_through, watermark].

        Two filters per timestamp: a coarse range on the raw column using the
        shortest/longest SLA, which Iceberg can prune files with (an expression like
        `ts + CASE ... END` defeats min/max statistics), then the exact per-currency
        deadline check on the surviving rows. With no overrides the two coincide.
        """
        s = self.settings
        start = self.evaluated_through or (
            self.watermark - timedelta(minutes=s.horizon_minutes)
        )
        lo, hi = F.lit(start), F.lit(self.watermark)
        coarse_lo, coarse_hi = F.lit(start - self.max_sla), F.lit(self.watermark - self.min_sla)
        sla = sla_interval(F.col("currency"), s.sla_minutes, s.sla_minutes_by_currency)

        def crossed(col: str):
            deadline = F.col(col) + sla
            return ((F.col(col) > coarse_lo) & (F.col(col) <= coarse_hi)
                    & (deadline > lo) & (deadline <= hi))

        due_bookings = bookings.where(crossed("confirmed_at") | crossed("cancelled_at"))
        due_orphans = payments.where((F.col("event_type") == CAPTURED) & crossed("occurred_at"))
        return due_bookings.select("booking_id").unionByName(due_orphans.select("booking_id"))

    def _reconcile(self, touched: DataFrame) -> dict:
        s = self.settings
        bookings = self.store.bookings()
        payments = self.store.payments()

        scope = (
            touched.unionByName(self._deadline_scope(bookings, payments)).distinct().persist()
        )
        try:
            evaluated = scope.count()
            if evaluated == 0:
                self.evaluated_through = self.watermark
                return {"evaluated_bookings": 0, "new_exceptions": 0, "resolved_exceptions": 0}

            current = reconcile(
                bookings.join(scope, "booking_id", "left_semi"),
                payments.join(scope, "booking_id", "left_semi"),
                bookings.select("booking_id").join(scope, "booking_id", "left_semi"),
                as_of=self.watermark,
                sla_minutes=s.sla_minutes,
                sla_minutes_by_currency=s.sla_minutes_by_currency,
                refund_tolerance_cents=s.refund_tolerance_cents,
            )
            existing = self.store.exceptions().join(scope, "booking_id", "left_semi")
            plan = plan_exception_changes(
                current, existing, scope, evaluated_at=self.clock(), detected_by="stream"
            ).persist()
            try:
                plan_stats = plan.groupBy("change", "alert").count().collect()
                if plan_stats:
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

        self.evaluated_through = self.watermark
        new = sum(r["count"] for r in plan_stats if r["change"] == "UPSERT" and r["alert"])
        resolved = sum(r["count"] for r in plan_stats if r["change"] == "RESOLVE")
        return {"evaluated_bookings": evaluated, "new_exceptions": new,
                "resolved_exceptions": resolved}

    def _record_file_growth(self, stats: dict) -> None:
        """Add this batch's new data/delete files to `stats` and refresh the open
        delete-file count, from each table's newest snapshot summary.

        A snapshot is counted only the first time it is seen, and compaction
        snapshots ('replace') are not counted as growth. Called once at startup
        to remember the current snapshots without counting them.
        """
        try:
            current = self.store.file_stats()
        except Exception:  # metadata is for tuning; it must never stop the stream
            log.warning("could not read snapshot summaries; compacting on batch count only",
                        exc_info=True)
            self.open_delete_files = 0
            return
        added_data = added_deletes = 0
        for table, fs in current.items():
            if fs["snapshot_id"] != self.seen_snapshots.get(table) and fs["operation"] != "replace":
                added_data += fs["added_data_files"]
                added_deletes += fs["added_delete_files"]
            self.seen_snapshots[table] = fs["snapshot_id"]
        self.open_delete_files = max((fs["open_delete_files"] for fs in current.values()),
                                     default=0)
        stats["added_data_files"] = added_data
        stats["added_delete_files"] = added_deletes
        stats["open_delete_files"] = self.open_delete_files

    def _maybe_compact(self, stats: dict) -> None:
        if stats.get("input_rows", 0) == 0:
            return
        s = self.settings
        self.batches_since_compaction += 1
        threshold = 0 if self.delete_trigger_paused else s.compact_delete_file_threshold
        reason = compaction_reason(self.batches_since_compaction, s.compact_every_batches,
                                   self.open_delete_files, threshold)
        if reason is None:
            return
        log.info("compacting: %s (open delete files %d, batches since last %d)",
                 reason, self.open_delete_files, self.batches_since_compaction)
        self.batches_since_compaction = 0
        started = time.time()
        try:
            self.store.maintain(older_than=self.clock() - timedelta(minutes=30))
            stats["compaction_s"] = time.time() - started
            self.delete_trigger_paused = False
        except Exception:  # maintenance must never stop the stream
            self.delete_trigger_paused = True
            log.exception("table maintenance failed; will retry on batch count")

    def _open_by_type(self) -> dict[str, tuple[int, int]]:
        rows = (
            self.store.exceptions()
            .where(F.col("status") == "OPEN")
            .groupBy("exception_type")
            .agg(F.count("*").alias("n"), F.sum("amount_at_risk_cents").alias("risk"))
            .collect()
        )
        return {r.exception_type: (r.n, r.risk or 0) for r in rows}
