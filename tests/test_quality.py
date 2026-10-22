from datetime import datetime

from reconstream.quality import booking_expectations, payment_expectations, validate

T = datetime(2026, 1, 1, 12, 0)
PAYMENTS = (
    "event_id string, event_type string, payment_id string, booking_id string, "
    "amount_cents long, currency string, occurred_at timestamp"
)
BOOKINGS = "booking_id string, status string, amount_cents long, currency string, confirmed_at timestamp"


def test_clean_payments_pass(spark):
    df = spark.createDataFrame(
        [("e1", "payment.captured", "p1", "b1", 100, "USD", T),
         ("e2", "refund.succeeded", "r1", "b1", 100, "USD", T)], PAYMENTS)
    out = validate(df, "payments", payment_expectations(), unique_key="event_id")
    assert out["success"], out


def test_bad_payments_are_reported_by_column(spark):
    df = spark.createDataFrame(
        [("e1", "payment.captured", "p1", "b1", 100, "USD", T),
         ("e1", "payment.captured", "p1", "b1", -5, "XYZ", T)], PAYMENTS)
    out = validate(df, "payments", payment_expectations(), unique_key="event_id")
    assert not out["success"]
    failed = {(f["expectation"], f["column"]) for f in out["failures"]}
    assert ("unique_key", "event_id") in failed
    assert ("expect_column_values_to_be_between", "amount_cents") in failed
    assert ("expect_column_values_to_be_in_set", "currency") in failed


def test_confirmed_booking_without_timestamp_fails(spark):
    df = spark.createDataFrame(
        [("b1", "CONFIRMED", 100, "USD", None), ("b2", "PENDING", 100, "USD", None)], BOOKINGS)
    out = validate(df, "bookings", booking_expectations(), unique_key="booking_id")
    assert [f["column"] for f in out["failures"]] == ["confirmed_at"]
