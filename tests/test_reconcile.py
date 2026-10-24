from datetime import datetime, timedelta, timezone

import pytest

from reconstream import reconcile as R

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
SLA = 2  # minutes

BOOKING_SCHEMA = (
    "booking_id string, status string, amount_cents long, currency string, "
    "confirmed_at timestamp, cancelled_at timestamp, is_deleted boolean"
)
PAYMENT_SCHEMA = (
    "event_id string, event_type string, payment_id string, booking_id string, "
    "amount_cents long, currency string, occurred_at timestamp, received_at timestamp"
)


def booking(bid, status="CONFIRMED", amount=10000, ccy="USD", confirmed=T0, cancelled=None,
            deleted=False):
    return (bid, status, amount, ccy, confirmed, cancelled, deleted)


_seq = iter(range(10_000))


def pay(bid, pid, amount=10000, ccy="USD", at=T0 + timedelta(seconds=10),
        etype=R.CAPTURED, event_id=None):
    eid = event_id or f"evt_{next(_seq)}"
    return (eid, etype, pid, bid, amount, ccy, at, at)


def run(spark, bookings, payments, as_of=T0 + timedelta(minutes=10), known=None,
        sla_by_currency=None):
    b = spark.createDataFrame(bookings, BOOKING_SCHEMA)
    p = spark.createDataFrame(payments, PAYMENT_SCHEMA)
    k = known if known is not None else b.select("booking_id")
    rows = R.reconcile(b, p, k, as_of, SLA, sla_by_currency).collect()
    return {(r.booking_id, r.exception_type): r for r in rows}


def test_clean_booking_has_no_exceptions(spark):
    assert run(spark, [booking("b1")], [pay("b1", "p1")]) == {}


def test_duplicate_delivery_of_one_capture_is_not_a_double_charge(spark):
    p = pay("b1", "p1")
    same_payment_new_delivery = pay("b1", "p1")
    assert run(spark, [booking("b1")], [p, same_payment_new_delivery]) == {}


def test_two_payment_ids_is_a_duplicate_charge(spark):
    out = run(spark, [booking("b1")], [pay("b1", "p1"), pay("b1", "p2")])
    exc = out[("b1", R.DUPLICATE_CHARGE)]
    assert exc.actual_amount_cents == 20000
    assert exc.amount_at_risk_cents == 10000
    assert exc.payment_ids == "p1,p2"
    assert len(out) == 1


def test_amount_mismatch(spark):
    out = run(spark, [booking("b1")], [pay("b1", "p1", amount=9500)])
    assert out[("b1", R.AMOUNT_MISMATCH)].amount_at_risk_cents == 500


def test_currency_mismatch_takes_priority_over_amount(spark):
    out = run(spark, [booking("b1")], [pay("b1", "p1", ccy="EUR")])
    assert set(out) == {("b1", R.CURRENCY_MISMATCH)}


def test_missing_payment_only_after_sla(spark):
    before_deadline = T0 + timedelta(minutes=SLA) - timedelta(seconds=1)
    assert run(spark, [booking("b1")], [], as_of=before_deadline) == {}
    out = run(spark, [booking("b1")], [], as_of=T0 + timedelta(minutes=SLA))
    exc = out[("b1", R.MISSING_PAYMENT)]
    assert exc.due_at == (T0 + timedelta(minutes=SLA)).replace(tzinfo=None)


INR_SLA = {"INR": 5}  # slower rail: INR bookings get 5 minutes instead of the default 2


def test_currency_override_delays_missing_payment_for_that_currency_only(spark):
    rows = [booking("usd"), booking("inr", ccy="INR")]
    at_default_deadline = T0 + timedelta(minutes=SLA)
    out = run(spark, rows, [], as_of=at_default_deadline, sla_by_currency=INR_SLA)
    assert set(out) == {("usd", R.MISSING_PAYMENT)}, "INR is still inside its 5-minute SLA"

    just_before = T0 + timedelta(minutes=5) - timedelta(seconds=1)
    assert ("inr", R.MISSING_PAYMENT) not in run(
        spark, rows, [], as_of=just_before, sla_by_currency=INR_SLA
    )
    out = run(spark, rows, [], as_of=T0 + timedelta(minutes=5), sla_by_currency=INR_SLA)
    assert out[("inr", R.MISSING_PAYMENT)].due_at == (T0 + timedelta(minutes=5)).replace(tzinfo=None)
    assert out[("usd", R.MISSING_PAYMENT)].due_at == at_default_deadline.replace(tzinfo=None)


def test_currency_override_matches_case_insensitively(spark):
    out = run(spark, [booking("inr", ccy="INR")], [], as_of=T0 + timedelta(minutes=SLA),
              sla_by_currency={"inr": 5})
    assert out == {}


def test_currency_override_applies_to_refund_deadline(spark):
    cancelled = T0 + timedelta(minutes=1)
    rows = [booking("inr", status="CANCELLED", ccy="INR", cancelled=cancelled)]
    payments = [pay("inr", "p1", ccy="INR")]
    assert run(spark, rows, payments, as_of=cancelled + timedelta(minutes=4),
               sla_by_currency=INR_SLA) == {}
    out = run(spark, rows, payments, as_of=cancelled + timedelta(minutes=5),
              sla_by_currency=INR_SLA)
    assert set(out) == {("inr", R.REFUND_MISMATCH)}


def test_orphan_payment_uses_its_capture_currency_sla(spark):
    p = [pay("ghost", "p9", ccy="INR", at=T0)]
    assert run(spark, [], p, as_of=T0 + timedelta(minutes=SLA), sla_by_currency=INR_SLA) == {}
    out = run(spark, [], p, as_of=T0 + timedelta(minutes=5), sla_by_currency=INR_SLA)
    assert set(out) == {("ghost", R.ORPHAN_PAYMENT)}


def test_pending_or_deleted_bookings_are_not_missing_payments(spark):
    rows = [booking("b1", status="PENDING", confirmed=None), booking("b2", deleted=True)]
    assert run(spark, rows, []) == {}


def test_refund_mismatch_when_cancelled_without_full_refund(spark):
    cancelled = T0 + timedelta(minutes=1)
    rows = [
        booking("full", status="CANCELLED", cancelled=cancelled),
        booking("partial", status="CANCELLED", cancelled=cancelled),
        booking("none", status="CANCELLED", cancelled=cancelled),
    ]
    payments = [
        pay("full", "p1"), pay("full", "r1", etype=R.REFUNDED),
        pay("partial", "p2"), pay("partial", "r2", amount=4000, etype=R.REFUNDED),
        pay("none", "p3"),
    ]
    out = run(spark, rows, payments)
    assert set(out) == {("partial", R.REFUND_MISMATCH), ("none", R.REFUND_MISMATCH)}
    assert out[("partial", R.REFUND_MISMATCH)].amount_at_risk_cents == 6000


def test_cancelled_before_payment_needs_no_refund(spark):
    rows = [booking("b1", status="CANCELLED", confirmed=None, cancelled=T0)]
    assert run(spark, rows, []) == {}


def test_orphan_payment_waits_for_sla_then_flags(spark):
    p = [pay("ghost", "p9", at=T0)]
    assert run(spark, [], p, as_of=T0 + timedelta(seconds=30)) == {}
    out = run(spark, [], p, as_of=T0 + timedelta(minutes=SLA))
    assert out[("ghost", R.ORPHAN_PAYMENT)].actual_amount_cents == 10000


def test_payment_for_known_booking_outside_scope_is_not_orphan(spark):
    known = spark.createDataFrame([("old",)], "booking_id string")
    assert run(spark, [], [pay("old", "p1")], known=known) == {}


def test_exception_ids_are_deterministic(spark):
    a = run(spark, [booking("b1")], [])
    b = run(spark, [booking("b1")], [])
    assert a[("b1", R.MISSING_PAYMENT)].exception_id == b[("b1", R.MISSING_PAYMENT)].exception_id


EXISTING_SCHEMA = (
    "exception_id string, booking_id string, exception_type string, status string, "
    "expected_amount_cents long, actual_amount_cents long, amount_at_risk_cents long, "
    "booking_currency string, payment_currency string, payment_ids string, due_at timestamp"
)


def _existing_row(exc, status):
    return (exc.exception_id, exc.booking_id, exc.exception_type, status,
            exc.expected_amount_cents, exc.actual_amount_cents, exc.amount_at_risk_cents,
            exc.booking_currency, exc.payment_currency, exc.payment_ids, exc.due_at)


@pytest.fixture
def missing_b1(spark):
    b = spark.createDataFrame([booking("b1")], BOOKING_SCHEMA)
    p = spark.createDataFrame([], PAYMENT_SCHEMA)
    return R.reconcile(b, p, b.select("booking_id"), T0 + timedelta(minutes=10), SLA)


def test_plan_new_exception_raises_alert(spark, missing_b1):
    existing = spark.createDataFrame([], EXISTING_SCHEMA)
    scope = spark.createDataFrame([("b1",)], "booking_id string")
    plan = R.plan_exception_changes(missing_b1, existing, scope, T0, "stream").collect()
    assert [(r.change, r.alert, r.detected_by) for r in plan] == [("UPSERT", True, "stream")]


def test_plan_existing_open_exception_does_not_alert_again(spark, missing_b1):
    existing = spark.createDataFrame([_existing_row(missing_b1.first(), "OPEN")], EXISTING_SCHEMA)
    scope = spark.createDataFrame([("b1",)], "booking_id string")
    plan = R.plan_exception_changes(missing_b1, existing, scope, T0, "stream").collect()
    assert [(r.change, r.alert) for r in plan] == [("UPSERT", False)]


def test_plan_resolves_when_late_payment_fixes_it(spark, missing_b1):
    existing = spark.createDataFrame([_existing_row(missing_b1.first(), "OPEN")], EXISTING_SCHEMA)
    scope = spark.createDataFrame([("b1",)], "booking_id string")
    nothing_now = missing_b1.limit(0)
    plan = R.plan_exception_changes(nothing_now, existing, scope, T0, "stream").collect()
    assert [(r.change, r.booking_id) for r in plan] == [("RESOLVE", "b1")]


def test_plan_never_resolves_outside_evaluated_scope(spark, missing_b1):
    existing = spark.createDataFrame([_existing_row(missing_b1.first(), "OPEN")], EXISTING_SCHEMA)
    scope = spark.createDataFrame([("other",)], "booking_id string")
    plan = R.plan_exception_changes(missing_b1.limit(0), existing, scope, T0, "stream").collect()
    assert plan == []


def test_plan_reopened_exception_alerts(spark, missing_b1):
    existing = spark.createDataFrame(
        [_existing_row(missing_b1.first(), "RESOLVED")], EXISTING_SCHEMA
    )
    scope = spark.createDataFrame([("b1",)], "booking_id string")
    plan = R.plan_exception_changes(missing_b1, existing, scope, T0, "batch").collect()
    assert [(r.change, r.alert) for r in plan] == [("UPSERT", True)]
