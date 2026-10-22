from datetime import date, datetime, timedelta, timezone

from memory_store import MemoryStore
from test_stream_processor import settings

from reconstream.batch import run_batch

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
NAIVE = T0.replace(tzinfo=None)


def _booking(store, bid, status="CONFIRMED", created=NAIVE):
    store.b[bid] = dict(
        booking_id=bid, guest_id="g", listing_id="l", status=status, amount_cents=5000,
        currency="USD", created_at=created, confirmed_at=created, cancelled_at=None,
        updated_at=created, is_deleted=False, source_lsn=1, cdc_op="c", ingested_at=created,
    )


def test_batch_catches_timeouts_the_stream_never_judged(spark):
    store = MemoryStore(spark)
    _booking(store, "quiet_night")
    stats = run_batch(store, settings(), now=T0 + timedelta(hours=8))
    assert stats["new_exceptions"] == 1
    (exc,) = store.e.values()
    assert (exc["exception_type"], exc["detected_by"]) == ("MISSING_PAYMENT", "batch")
    assert len(store.alerts) == 1


def test_batch_is_idempotent(spark):
    store = MemoryStore(spark)
    _booking(store, "b1")
    run_batch(store, settings(), now=T0 + timedelta(hours=8))
    again = run_batch(store, settings(), now=T0 + timedelta(hours=9))
    assert again["new_exceptions"] == 0 and again["still_open"] == 1
    assert len(store.e) == 1


def test_date_range_limits_scope(spark):
    store = MemoryStore(spark)
    _booking(store, "in_range")
    _booking(store, "older", created=NAIVE - timedelta(days=3))
    run_batch(store, settings(), start=date(2026, 1, 1), end=date(2026, 1, 2),
              now=T0 + timedelta(hours=8))
    assert {e["booking_id"] for e in store.e.values()} == {"in_range"}
