import json
from datetime import datetime, timezone

from reconstream.parsing import (
    latest_booking_per_key,
    parse_booking_changes,
    parse_payment_events,
)

KAFKA_TS = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _raw(spark, values):
    return spark.createDataFrame(
        [(json.dumps(v).encode() if v is not None else None, KAFKA_TS) for v in values],
        "value binary, timestamp timestamp",
    )


def _booking(status, lsn, op="u", updated="2026-01-01T12:00:00.000000Z"):
    row = {
        "booking_id": "bk_1",
        "guest_id": "g_1",
        "listing_id": "l_1",
        "status": status,
        "amount_cents": 25000,
        "currency": "USD",
        "created_at": "2026-01-01T11:59:00.000000Z",
        "confirmed_at": "2026-01-01T12:00:00.123456Z" if status != "PENDING" else None,
        "cancelled_at": None,
        "updated_at": updated,
    }
    return {
        "before": row if op == "d" else None,
        "after": None if op == "d" else row,
        "source": {"lsn": lsn, "ts_ms": 1},
        "op": op,
        "ts_ms": 1,
    }


def test_parses_debezium_envelope_and_timestamps(spark):
    out = parse_booking_changes(_raw(spark, [_booking("CONFIRMED", 10)])).collect()
    assert len(out) == 1
    r = out[0]
    assert r.booking_id == "bk_1"
    assert r.status == "CONFIRMED"
    assert r.amount_cents == 25000
    assert r.is_deleted is False
    assert r.confirmed_at == datetime(2026, 1, 1, 12, 0, 0, 123456)


def test_delete_uses_before_image_and_flags_row(spark):
    r = parse_booking_changes(_raw(spark, [_booking("CONFIRMED", 11, op="d")])).collect()[0]
    assert r.booking_id == "bk_1"
    assert r.is_deleted is True


def test_latest_change_wins_by_lsn_not_arrival_order(spark):
    changes = parse_booking_changes(
        _raw(spark, [_booking("CONFIRMED", 20), _booking("PENDING", 5)])
    )
    rows = latest_booking_per_key(changes).collect()
    assert [(r.status, r.source_lsn) for r in rows] == [("CONFIRMED", 20)]


def test_malformed_json_is_dropped(spark):
    raw = spark.createDataFrame([(b"not json", KAFKA_TS)], "value binary, timestamp timestamp")
    assert parse_booking_changes(raw).count() == 0
    assert parse_payment_events(raw).count() == 0


def _payment(event_id="evt_1", **over):
    p = {
        "event_id": event_id,
        "type": "payment.captured",
        "payment_id": "pay_1",
        "booking_id": "bk_1",
        "amount_cents": 25000,
        "currency": "USD",
        "occurred_at": "2026-01-01T12:00:05Z",
        "received_at": "2026-01-01T12:00:06Z",
    }
    p.update(over)
    return p


def test_duplicate_deliveries_collapse_on_event_id(spark):
    rows = parse_payment_events(
        _raw(spark, [_payment(), _payment(), _payment(event_id="evt_2", payment_id="pay_2")])
    ).collect()
    assert sorted(r.event_id for r in rows) == ["evt_1", "evt_2"]


def test_unknown_event_type_and_missing_keys_are_rejected(spark):
    rows = parse_payment_events(
        _raw(spark, [_payment(type="charge.weird"), _payment(event_id="evt_3", booking_id=None)])
    ).collect()
    assert rows == []
