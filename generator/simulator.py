"""Marketplace traffic simulator with deliberate, recorded faults.

It behaves like a booking system plus a payment provider:
  * bookings are created, confirmed, sometimes cancelled (written to Postgres)
  * the provider sends payment and refund webhooks with realistic noise:
    random delivery delays (out of order), duplicate deliveries of the same
    event, failed attempts before a successful capture, and malformed payloads
  * a small share of bookings get a real problem injected, and every injected
    problem is written to a ground-truth table so detection can be measured

Noise is not a fault: a duplicate delivery or a late-but-valid payment must
NOT produce an open exception. Faults are the six real mismatches.
"""

from __future__ import annotations

import heapq
import itertools
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Protocol

# Fault -> the exception the pipeline is expected to raise for it.
FAULTS = {
    "DUPLICATE_CHARGE": "DUPLICATE_CHARGE",
    "AMOUNT_MISMATCH": "AMOUNT_MISMATCH",
    "CURRENCY_MISMATCH": "CURRENCY_MISMATCH",
    "MISSING_PAYMENT": "MISSING_PAYMENT",
    "REFUND_MISMATCH": "REFUND_MISMATCH",
    "ORPHAN_PAYMENT": "ORPHAN_PAYMENT",
    # Not a fault: payment arrives after the SLA. The pipeline should flag it,
    # then resolve it on its own when the payment lands.
    "LATE_PAYMENT": "MISSING_PAYMENT",
}

DEFAULT_FAULT_RATES = {
    "DUPLICATE_CHARGE": 0.020,
    "AMOUNT_MISMATCH": 0.015,
    "CURRENCY_MISMATCH": 0.005,
    "MISSING_PAYMENT": 0.015,
    "REFUND_MISMATCH": 0.010,
    "LATE_PAYMENT": 0.010,
}

CURRENCIES = (("USD", 0.70), ("EUR", 0.15), ("GBP", 0.10), ("INR", 0.05))


class Sink(Protocol):
    def insert_booking(self, row: dict) -> None: ...
    def update_booking(self, booking_id: str, changes: dict) -> None: ...
    def deliver(self, event: dict) -> None: ...
    def record_fault(self, fault: dict) -> None: ...


@dataclass
class Config:
    bookings_per_second: float = 2.0
    sla: timedelta = timedelta(minutes=2)
    fault_rates: dict = field(default_factory=lambda: dict(DEFAULT_FAULT_RATES))
    orphan_rate: float = 0.008  # per booking, an extra capture for a booking that never existed
    duplicate_delivery_rate: float = 0.05
    malformed_rate: float = 0.005
    failed_attempt_rate: float = 0.03
    cancel_rate: float = 0.05
    abandon_rate: float = 0.02
    max_delivery_delay: timedelta = timedelta(seconds=20)


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Simulator:
    def __init__(self, sink: Sink, config: Config, seed: int | None = None):
        self.sink = sink
        self.cfg = config
        self.rng = random.Random(seed)
        self._queue: list = []
        self._seq = itertools.count()
        self._spawn_credit = 0.0
        self._last_tick: datetime | None = None
        self.counts: dict[str, int] = {}

    # ---------- scheduling ----------
    def _at(self, when: datetime, fn: Callable[[], None]) -> None:
        heapq.heappush(self._queue, (when, next(self._seq), fn))

    def tick(self, now: datetime) -> None:
        if self._last_tick is not None:
            self._spawn_credit += (now - self._last_tick).total_seconds() * self.cfg.bookings_per_second
        self._last_tick = now
        while self._spawn_credit >= 1:
            self._spawn_credit -= 1
            self._new_booking(now)
        while self._queue and self._queue[0][0] <= now:
            _, _, fn = heapq.heappop(self._queue)
            fn()

    def drain(self) -> None:
        """Run everything still scheduled (used by tests and at shutdown)."""
        while self._queue:
            _, _, fn = heapq.heappop(self._queue)
            fn()

    def _count(self, key: str) -> None:
        self.counts[key] = self.counts.get(key, 0) + 1

    # ---------- helpers ----------
    def _id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.UUID(int=self.rng.getrandbits(128)).hex[:16]}"

    def _delay(self, lo: float, hi: float) -> timedelta:
        return timedelta(seconds=self.rng.uniform(lo, hi))

    def _pick_fault(self) -> str | None:
        r = self.rng.random()
        for fault, rate in self.cfg.fault_rates.items():
            if r < rate:
                return fault
            r -= rate
        return None

    def _currency(self) -> str:
        r = self.rng.random()
        for ccy, w in CURRENCIES:
            if r < w:
                return ccy
            r -= w
        return "USD"

    def _record(self, booking_id: str, fault: str, injected_at: datetime,
                detectable_at: datetime, **details) -> None:
        self._count(f"fault:{fault}")
        self.sink.record_fault({
            "booking_id": booking_id,
            "fault_type": fault,
            "expected_exception": FAULTS[fault],
            "expect_resolved": fault == "LATE_PAYMENT",
            "injected_at": injected_at,
            "detectable_at": detectable_at,
            "details": details,
        })

    def _send(self, event: dict, occurred: datetime) -> datetime:
        """Schedule delivery of one provider event. Returns when it will arrive."""
        event = dict(event, occurred_at=iso(occurred))
        arrives = occurred + self._delay(0, self.cfg.max_delivery_delay.total_seconds())
        self._at(arrives, lambda: self._deliver(event))
        if self.rng.random() < self.cfg.duplicate_delivery_rate:
            again = arrives + self._delay(1, 30)
            self._at(again, lambda: self._deliver(event, duplicate=True))
        if self.rng.random() < self.cfg.malformed_rate:
            broken = dict(event, event_id=self._id("evt"),
                          amount_cents=-abs(event["amount_cents"]) - 1)
            self._at(arrives, lambda: self._deliver(broken, malformed=True))
        return arrives

    def _deliver(self, event: dict, duplicate: bool = False, malformed: bool = False) -> None:
        self._count("delivery:duplicate" if duplicate else
                    "delivery:malformed" if malformed else "delivery")
        self.sink.deliver(event)

    def _capture(self, booking_id: str, amount: int, currency: str,
                 occurred: datetime) -> tuple[str, datetime]:
        payment_id = self._id("pay")
        arrives = self._send({
            "event_id": self._id("evt"), "type": "payment.captured", "payment_id": payment_id,
            "booking_id": booking_id, "amount_cents": amount, "currency": currency,
        }, occurred)
        return payment_id, arrives

    def _refund(self, booking_id: str, amount: int, currency: str, occurred: datetime) -> datetime:
        return self._send({
            "event_id": self._id("evt"), "type": "refund.succeeded", "payment_id": self._id("re"),
            "booking_id": booking_id, "amount_cents": amount, "currency": currency,
        }, occurred)

    # ---------- lifecycle ----------
    def _new_booking(self, now: datetime) -> None:
        self._count("bookings")
        booking_id = self._id("bk")
        amount = self.rng.randrange(50, 1500) * 100
        currency = self._currency()
        self.sink.insert_booking({
            "booking_id": booking_id, "guest_id": self._id("g"), "listing_id": self._id("l"),
            "status": "PENDING", "amount_cents": amount, "currency": currency,
            "created_at": now, "updated_at": now,
        })

        if self.rng.random() < self.cfg.orphan_rate:
            ghost = self._id("bk")
            occurred = now + self._delay(1, 10)
            self._at(occurred, lambda: self._capture(ghost, amount, currency, occurred))
            self._record(ghost, "ORPHAN_PAYMENT", now, occurred + self.cfg.sla)

        if self.rng.random() < self.cfg.abandon_rate:
            cancel_at = now + self._delay(2, 10)
            self._at(cancel_at, lambda: self.sink.update_booking(booking_id, {
                "status": "CANCELLED", "cancelled_at": cancel_at, "updated_at": cancel_at}))
            return

        fault = self._pick_fault()
        confirm_at = now + self._delay(0.5, 3)
        self._at(confirm_at, lambda: self._confirmed(booking_id, amount, currency,
                                                     confirm_at, fault))

    def _confirmed(self, booking_id: str, amount: int, currency: str, at: datetime,
                   fault: str | None) -> None:
        self.sink.update_booking(booking_id, {"status": "CONFIRMED", "confirmed_at": at,
                                              "updated_at": at})
        sla = self.cfg.sla
        pay_at = at + self._delay(1, 15)

        if fault == "MISSING_PAYMENT":
            self._record(booking_id, fault, at, at + sla)
            return

        if fault == "LATE_PAYMENT":
            late = at + sla + self._delay(30, 90)
            self._at(late, lambda: self._capture(booking_id, amount, currency, late))
            self._record(booking_id, fault, at, at + sla, payment_at=iso(late))
            return

        if self.rng.random() < self.cfg.failed_attempt_rate:
            fail_at = pay_at - timedelta(seconds=1)
            self._send({
                "event_id": self._id("evt"), "type": "payment.failed",
                "payment_id": self._id("pay"), "booking_id": booking_id,
                "amount_cents": amount, "currency": currency,
            }, fail_at)

        def capture():
            if fault == "AMOUNT_MISMATCH":
                wrong = max(100, amount + self.rng.choice([-1, 1]) * self.rng.randrange(1, 40) * 100)
                _, arrives = self._capture(booking_id, wrong, currency, pay_at)
                self._record(booking_id, fault, pay_at, arrives, charged=wrong, expected=amount)
            elif fault == "CURRENCY_MISMATCH":
                other = self.rng.choice([c for c, _ in CURRENCIES if c != currency])
                _, arrives = self._capture(booking_id, amount, other, pay_at)
                self._record(booking_id, fault, pay_at, arrives, charged_currency=other)
            elif fault == "DUPLICATE_CHARGE":
                _, first = self._capture(booking_id, amount, currency, pay_at)
                second_at = pay_at + self._delay(5, 60)
                _, second = self._capture(booking_id, amount, currency, second_at)
                self._record(booking_id, fault, second_at, max(first, second))
            else:
                self._capture(booking_id, amount, currency, pay_at)
                wants_cancel = fault == "REFUND_MISMATCH" or self.rng.random() < self.cfg.cancel_rate
                if wants_cancel:
                    cancel_at = at + self._delay(20, 60)
                    self._at(cancel_at, lambda: self._cancelled(booking_id, amount, currency,
                                                                cancel_at, fault))

        self._at(pay_at, capture)

    def _cancelled(self, booking_id: str, amount: int, currency: str, at: datetime,
                   fault: str | None) -> None:
        self.sink.update_booking(booking_id, {"status": "CANCELLED", "cancelled_at": at,
                                              "updated_at": at})
        refund_at = at + self._delay(2, 20)
        if fault == "REFUND_MISMATCH":
            if self.rng.random() < 0.5:
                partial = max(100, amount // 2)
                self._at(refund_at, lambda: self._refund(booking_id, partial, currency, refund_at))
                self._record(booking_id, fault, at, at + self.cfg.sla, refunded=partial)
            else:
                self._record(booking_id, fault, at, at + self.cfg.sla, refunded=0)
        else:
            self._at(refund_at, lambda: self._refund(booking_id, amount, currency, refund_at))
