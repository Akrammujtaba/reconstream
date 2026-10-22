"""Drive the stream processor through several micro-batches with an in-memory store."""

import json
from datetime import datetime, timedelta, timezone

from memory_store import MemoryStore

from reconstream.config import Settings
from reconstream.observability import StreamMetrics
from reconstream.stream_processor import StreamProcessor

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
BOOKINGS_TOPIC = "pg.public.bookings"
PAYMENTS_TOPIC = "payments.raw"


def settings(**over):
    base = dict(
        kafka_bootstrap="unused", bookings_topic=BOOKINGS_TOPIC, payments_topic=PAYMENTS_TOPIC,
        alerts_topic="recon.alerts", catalog="lake", checkpoint_dir="/tmp/x", sla_minutes=2,
        allowed_lateness_seconds=30, horizon_minutes=360, trigger_seconds=10,
        max_offsets_per_trigger=1000, pushgateway=None, slack_webhook_url=None,
    )
    base.update(over)
    return Settings(**base)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


_offset = iter(range(100_000))


def cdc(booking_id, status, at, lsn, amount=10000):
    row = {
        "booking_id": booking_id, "guest_id": "g", "listing_id": "l", "status": status,
        "amount_cents": amount, "currency": "USD", "created_at": iso(T0),
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


def make(spark):
    store = MemoryStore(spark)
    clock = Clock(T0)
    proc = StreamProcessor(store, settings(), StreamMetrics(None), clock=clock)
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
