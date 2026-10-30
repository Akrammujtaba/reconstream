"""Drive the stream processor through several micro-batches with an in-memory store."""

import json
from datetime import datetime, timedelta, timezone

import pytest
from memory_store import MemoryStore

from reconstream.config import Settings
from reconstream.lakehouse import file_stats_from_snapshot
from reconstream.observability import StreamMetrics
from reconstream.stream_processor import StreamProcessor, compaction_reason

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
BOOKINGS_TOPIC = "pg.public.bookings"
PAYMENTS_TOPIC = "payments.raw"


def settings(**over):
    base = dict(
        kafka_bootstrap="unused", bookings_topic=BOOKINGS_TOPIC, payments_topic=PAYMENTS_TOPIC,
        alerts_topic="recon.alerts", catalog="lake", checkpoint_dir="/tmp/x", sla_minutes=2,
        allowed_lateness_seconds=30, horizon_minutes=360, trigger_seconds=10,
        max_offsets_per_trigger=1000, compact_every_batches=0, pushgateway=None, slack_webhook_url=None,
    )
    base.update(over)
    return Settings(**base)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


_offset = iter(range(100_000))


def cdc(booking_id, status, at, lsn, amount=10000, currency="USD"):
    row = {
        "booking_id": booking_id, "guest_id": "g", "listing_id": "l", "status": status,
        "amount_cents": amount, "currency": currency, "created_at": iso(T0),
        "confirmed_at": iso(at) if status == "CONFIRMED" else None,
        "cancelled_at": None, "updated_at": iso(at),
    }
    value = {"before": None, "after": row, "source": {"lsn": lsn, "ts_ms": 0}, "op": "u", "ts_ms": 0}
    return (BOOKINGS_TOPIC, value, at)


def payment(event_id, booking_id, at, payment_id="pay_1", amount=10000):
    value = {
        "event_id": event_id, "type": "payment.captured", "payment_id": payment_id,
        "booking_id": booking_id, "amount_cents": amount, "currency": "USD",
        "occurred_at": iso(at), "received_at": iso(at),
    }
    return (PAYMENTS_TOPIC, value, at)


def batch(spark, records):
    rows = [
        (topic, 0, next(_offset), None, json.dumps(v).encode(), ts.replace(tzinfo=None))
        for topic, v, ts in records
    ]
    return spark.createDataFrame(
        rows,
        "topic string, partition int, offset long, key binary, value binary, timestamp timestamp",
    )


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


def make(spark, **over):
    store = MemoryStore(spark)
    clock = Clock(T0)
    proc = StreamProcessor(store, settings(**over), StreamMetrics(None), clock=clock)
    return store, proc, clock


def exc(store, booking_id):
    return {e["exception_type"]: e for e in store.e.values() if e["booking_id"] == booking_id}


def test_missing_payment_is_flagged_on_event_time_then_resolved_by_late_payment(spark):
    store, proc, clock = make(spark)

    proc.process(batch(spark, [cdc("b1", "CONFIRMED", T0, lsn=1)]), 0)
    assert store.e == {}, "SLA has not passed in event time yet"

    # Other traffic moves event time forward 3 minutes; watermark = T0 + 2m30s > deadline.
    clock.now = T0 + timedelta(minutes=3)
    proc.process(batch(spark, [cdc("b2", "CONFIRMED", T0 + timedelta(minutes=3), lsn=2)]), 1)
    assert exc(store, "b1")["MISSING_PAYMENT"]["status"] == "OPEN"
    assert [a["booking_id"] for a in store.alerts] == ["b1"]

    # The payment finally shows up; the same exception is resolved, not duplicated.
    clock.now = T0 + timedelta(minutes=4)
    proc.process(batch(spark, [payment("e1", "b1", T0 + timedelta(minutes=4))]), 2)
    b1 = exc(store, "b1")["MISSING_PAYMENT"]
    assert b1["status"] == "RESOLVED"
    assert b1["resolved_at"] is not None
    assert len(store.alerts) == 1


def test_wall_clock_alone_does_not_trigger_timeouts(spark):
    store, proc, clock = make(spark)
    clock.now = T0 + timedelta(hours=1)  # Spark running late must not invent missing payments
    proc.process(batch(spark, [cdc("b1", "CONFIRMED", T0, lsn=1)]), 0)
    assert store.e == {}


def test_redelivered_webhooks_are_counted_and_ignored(spark):
    store, proc, _ = make(spark)
    p = payment("e1", "b1", T0 + timedelta(seconds=5))
    proc.process(batch(spark, [cdc("b1", "CONFIRMED", T0, lsn=1), p, p]), 0)
    proc.process(batch(spark, [p]), 1)
    assert len(store.p) == 1
    assert proc.metrics.totals["duplicate_deliveries"] == 2
    assert exc(store, "b1") == {}


def test_double_charge_alerts_once_across_batches(spark):
    store, proc, _ = make(spark)
    proc.process(batch(spark, [
        cdc("b1", "CONFIRMED", T0, lsn=1),
        payment("e1", "b1", T0 + timedelta(seconds=5), payment_id="pay_a"),
    ]), 0)
    proc.process(batch(spark, [payment("e2", "b1", T0 + timedelta(seconds=20), payment_id="pay_b")]), 1)
    proc.process(batch(spark, [cdc("b9", "CONFIRMED", T0 + timedelta(seconds=30), lsn=2)]), 2)
    assert exc(store, "b1")["DUPLICATE_CHARGE"]["actual_amount_cents"] == 20000
    assert [a["exception_type"] for a in store.alerts] == ["DUPLICATE_CHARGE"]


def test_out_of_order_cdc_keeps_newest_state(spark):
    store, proc, _ = make(spark)
    proc.process(batch(spark, [cdc("b1", "CONFIRMED", T0, lsn=5)]), 0)
    proc.process(batch(spark, [cdc("b1", "PENDING", T0 - timedelta(seconds=5), lsn=3)]), 1)
    assert store.b["b1"]["status"] == "CONFIRMED"


def test_watermark_resumes_from_silver_after_restart(spark):
    store, proc, _ = make(spark)
    proc.process(batch(spark, [cdc("b1", "CONFIRMED", T0, lsn=1)]), 0)
    restarted = StreamProcessor(store, settings(), StreamMetrics(None), clock=Clock(T0))
    assert restarted.watermark == T0 - timedelta(seconds=30)


def test_empty_batch_is_a_no_op(spark):
    store, proc, _ = make(spark)
    proc.process(batch(spark, []), 0)
    assert store.bronze_rows == 0 and store.e == {}


def test_only_touched_or_newly_due_bookings_are_reevaluated(spark):
    store, proc, clock = make(spark)
    # 20 bookings confirmed at T0, all paid: nothing to flag, ever.
    events = []
    for i in range(20):
        events.append(cdc(f"b{i}", "CONFIRMED", T0, lsn=i + 1))
        events.append(payment(f"e{i}", f"b{i}", T0 + timedelta(seconds=5), payment_id=f"p{i}"))
    proc.process(batch(spark, events), 0)

    # Later batch touches one new booking; deadlines for the 20 have not moved past
    # since the last evaluation, so only the touched booking is re-checked.
    later = T0 + timedelta(seconds=40)
    proc.process(batch(spark, [cdc("new", "PENDING", later, lsn=100)]), 1)
    assert store.e == {}
    assert proc.last_stats["evaluated_bookings"] == 1
    assert proc.evaluated_through == later - timedelta(seconds=30)


def test_deadline_window_catches_booking_without_new_events(spark):
    store, proc, clock = make(spark)
    proc.process(batch(spark, [cdc("quiet", "CONFIRMED", T0, lsn=1)]), 0)
    # Unrelated traffic only; "quiet" gets no new events but its deadline passes.
    for n, minutes in enumerate((1, 2, 3)):
        at = T0 + timedelta(minutes=minutes)
        proc.process(batch(spark, [cdc(f"other{n}", "PENDING", at, lsn=10 + n)]), n + 1)
    assert exc(store, "quiet")["MISSING_PAYMENT"]["status"] == "OPEN"


def test_periodic_maintenance_runs_every_n_batches(spark):
    store = MemoryStore(spark)
    proc = StreamProcessor(store, settings(compact_every_batches=2), StreamMetrics(None),
                           clock=Clock(T0))
    for i in range(4):
        proc.process(batch(spark, [cdc(f"b{i}", "PENDING", T0, lsn=i + 1)]), i)
    assert store.maintenance_runs == 2


def snap(snapshot_id, open_deletes, added_data=1, added_deletes=1, operation="overwrite"):
    return {"snapshot_id": snapshot_id, "operation": operation, "added_data_files": added_data,
            "added_delete_files": added_deletes, "open_delete_files": open_deletes}


@pytest.mark.parametrize("open_deletes, expected", [(19, None), (20, "delete_files"),
                                                    (21, "delete_files")])
def test_compaction_reason_on_each_side_of_delete_threshold(open_deletes, expected):
    assert compaction_reason(batches_since=1, every=40, open_delete_files=open_deletes,
                             threshold=20) == expected


def test_compaction_reason_falls_back_to_batch_count():
    assert compaction_reason(39, every=40, open_delete_files=0, threshold=20) is None
    assert compaction_reason(40, every=40, open_delete_files=0, threshold=20) == "batch_count"
    # Threshold 0 means "batch count only", which is the old behaviour.
    assert compaction_reason(1, every=40, open_delete_files=500, threshold=0) is None
    assert compaction_reason(1, every=0, open_delete_files=0, threshold=0) is None


def test_compacts_when_delete_files_cross_threshold(spark):
    store, proc, _ = make(spark, compact_delete_file_threshold=3, compact_every_batches=100)
    store.snapshots = {"gold.recon_exceptions": snap(1, open_deletes=2)}
    proc.process(batch(spark, [cdc("b1", "PENDING", T0, lsn=1)]), 0)
    assert getattr(store, "maintenance_runs", 0) == 0
    assert proc.last_stats["open_delete_files"] == 2

    store.snapshots = {"gold.recon_exceptions": snap(2, open_deletes=3)}
    proc.process(batch(spark, [cdc("b2", "PENDING", T0, lsn=2)]), 1)
    assert store.maintenance_runs == 1
    assert proc.batches_since_compaction == 0


def test_file_growth_counts_each_snapshot_once_and_skips_compaction(spark):
    store = MemoryStore(spark)
    # Snapshots that exist before startup are remembered, not counted as this run's growth.
    store.snapshots = {"silver.bookings": snap(10, open_deletes=4, added_data=7)}
    proc = StreamProcessor(store, settings(), StreamMetrics(None), clock=Clock(T0))

    store.snapshots = {"silver.bookings": snap(11, open_deletes=5, added_data=2),
                       "gold.recon_exceptions": snap(20, open_deletes=1, added_deletes=1)}
    proc.process(batch(spark, [cdc("b1", "PENDING", T0, lsn=1)]), 0)
    assert (proc.last_stats["added_data_files"], proc.last_stats["added_delete_files"]) == (3, 2)
    assert proc.last_stats["open_delete_files"] == 5

    # gold had no new commit; bookings' newest snapshot is a compaction, not growth.
    store.snapshots = {"silver.bookings": snap(12, open_deletes=0, added_data=1,
                                               operation="replace"),
                       "gold.recon_exceptions": snap(20, open_deletes=1)}
    proc.process(batch(spark, [cdc("b2", "PENDING", T0, lsn=2)]), 1)
    assert (proc.last_stats["added_data_files"], proc.last_stats["added_delete_files"]) == (0, 0)
    assert proc.last_stats["open_delete_files"] == 1


def test_unreadable_snapshots_fall_back_to_batch_count(spark):
    store, proc, _ = make(spark, compact_delete_file_threshold=1, compact_every_batches=2)

    def broken():
        raise RuntimeError("metadata table unavailable")

    store.file_stats = broken
    for i in range(2):
        proc.process(batch(spark, [cdc(f"b{i}", "PENDING", T0, lsn=i + 1)]), i)
    assert store.maintenance_runs == 1


def test_failed_compaction_does_not_retry_every_batch(spark):
    store, proc, _ = make(spark, compact_delete_file_threshold=1, compact_every_batches=3)
    store.snapshots = {"gold.recon_exceptions": snap(1, open_deletes=5)}
    attempts = []

    def failing(older_than):
        attempts.append(older_than)
        raise RuntimeError("commit conflict")

    store.maintain = failing
    for i in range(3):
        proc.process(batch(spark, [cdc(f"b{i}", "PENDING", T0, lsn=i + 1)]), i)
    # Batch 0 tries on the delete count and fails; batches 1-2 wait for the batch count.
    assert len(attempts) == 1
    proc.process(batch(spark, [cdc("b3", "PENDING", T0, lsn=4)]), 3)
    assert len(attempts) == 2


def test_snapshot_summary_without_added_keys_counts_as_zero():
    # An empty MERGE still commits a snapshot; Iceberg omits the added-* keys then.
    assert file_stats_from_snapshot(7, "overwrite", {"total-delete-files": "5"}) == {
        "snapshot_id": 7, "operation": "overwrite", "added_data_files": 0,
        "added_delete_files": 0, "open_delete_files": 5,
    }
    assert file_stats_from_snapshot(8, "append", None)["open_delete_files"] == 0


def test_per_currency_sla_is_honoured_by_the_deadline_scope(spark):
    """An INR booking with a 5-minute SLA must not be flagged at the 2-minute default, and
    must still be picked up later by the deadline scan even though no event touches it."""
    store, proc, clock = make(spark, sla_minutes_by_currency={"INR": 5})
    proc.process(batch(spark, [
        cdc("usd", "CONFIRMED", T0, lsn=1),
        cdc("inr", "CONFIRMED", T0, lsn=2, currency="INR"),
    ]), 0)

    # Watermark T0 + 2m30s: past the USD deadline, not the INR one.
    clock.now = T0 + timedelta(minutes=3)
    proc.process(batch(spark, [cdc("x1", "PENDING", T0 + timedelta(minutes=3), lsn=3)]), 1)
    assert exc(store, "usd")["MISSING_PAYMENT"]["status"] == "OPEN"
    assert exc(store, "inr") == {}

    # Unrelated traffic moves the watermark to T0 + 5m30s; the INR deadline is crossed.
    clock.now = T0 + timedelta(minutes=6)
    proc.process(batch(spark, [cdc("x2", "PENDING", T0 + timedelta(minutes=6), lsn=4)]), 2)
    inr = exc(store, "inr")["MISSING_PAYMENT"]
    assert inr["status"] == "OPEN"
    assert inr["due_at"] == (T0 + timedelta(minutes=5)).replace(tzinfo=None)
    assert [a["booking_id"] for a in store.alerts] == ["usd", "inr"]
