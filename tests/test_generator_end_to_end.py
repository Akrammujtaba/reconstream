"""Generate traffic with injected faults, push it through parsing + reconciliation,
and check the rules find every injected fault and nothing else.

This is the same measurement the live evaluation does, minus Kafka and Iceberg.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "webhook"))
from app import PaymentEvent  # noqa: E402
from pydantic import ValidationError  # noqa: E402
from simulator import Config, Simulator  # noqa: E402

from reconstream.parsing import parse_payment_events  # noqa: E402
from reconstream.reconcile import reconcile  # noqa: E402

START = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def accepted_by_webhook(event):
    try:
        PaymentEvent.model_validate(event)
        return True
    except ValidationError:
        return False


class FakeSink:
    def __init__(self):
        self.bookings, self.events, self.faults = {}, [], []

    def insert_booking(self, row):
        self.bookings[row["booking_id"]] = dict(row, confirmed_at=None, cancelled_at=None)

    def update_booking(self, booking_id, changes):
        self.bookings[booking_id].update(changes)

    def deliver(self, event):
        self.events.append(event)

    def record_fault(self, fault):
        self.faults.append(fault)


@pytest.fixture(scope="module")
def run(spark):
    sink = FakeSink()
    cfg = Config(bookings_per_second=4)
    # Raise fault rates so a short run still has every type several times.
    cfg.fault_rates = {k: v * 4 for k, v in cfg.fault_rates.items()}
    cfg.orphan_rate *= 4
    sim = Simulator(sink, cfg, seed=7)
    now = START
    end = START + timedelta(minutes=15)
    while now < end:
        sim.tick(now)
        now += timedelta(milliseconds=500)
    sim.drain()
    as_of = end + timedelta(minutes=10)  # everything has had time to arrive

    bookings = spark.createDataFrame(
        [(b["booking_id"], b["status"], b["amount_cents"], b["currency"],
          b["confirmed_at"], b["cancelled_at"], False) for b in sink.bookings.values()],
        "booking_id string, status string, amount_cents long, currency string, "
        "confirmed_at timestamp, cancelled_at timestamp, is_deleted boolean",
    )
    raw = spark.createDataFrame(
        [(json.dumps(dict(e, received_at=e["occurred_at"])).encode(), START.replace(tzinfo=None))
         for e in sink.events if accepted_by_webhook(e)],
        "value binary, timestamp timestamp",
    )
    payments = parse_payment_events(raw)  # valid events only, duplicates collapsed
    found = reconcile(bookings, payments, bookings.select("booking_id"), as_of, 2).collect()
    return sim, sink, {(r.booking_id, r.exception_type) for r in found}


def test_webhook_contract_rejects_exactly_the_malformed_copies(run):
    sim, sink, _ = run
    rejected = sum(not accepted_by_webhook(e) for e in sink.events)
    assert rejected == sim.counts["delivery:malformed"]


def test_every_fault_type_was_injected(run):
    sim, _, _ = run
    for fault in ("DUPLICATE_CHARGE", "AMOUNT_MISMATCH", "CURRENCY_MISMATCH",
                  "MISSING_PAYMENT", "REFUND_MISMATCH", "LATE_PAYMENT", "ORPHAN_PAYMENT"):
        assert sim.counts.get(f"fault:{fault}", 0) >= 2, fault
    assert sim.counts["delivery:duplicate"] > 0
    assert sim.counts["delivery:malformed"] > 0


def test_rules_find_exactly_the_injected_faults(run):
    _, sink, found = run
    expected = {(f["booking_id"], f["expected_exception"])
                for f in sink.faults if not f["expect_resolved"]}
    assert found == expected


def test_late_payments_end_up_with_no_open_exception(run):
    _, sink, found = run
    late = {f["booking_id"] for f in sink.faults if f["fault_type"] == "LATE_PAYMENT"}
    assert late and not any(bid in late for bid, _ in found)
