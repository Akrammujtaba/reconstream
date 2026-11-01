import csv
import json
from datetime import datetime
from pathlib import Path

from reconstream.reconcile import EXCEPTION_COLUMNS, EXCEPTION_SEVERITY, EXCEPTION_TYPES
from reconstream.store import ALERT_COLUMNS, UNKNOWN_SEVERITY, alert_messages

SEED = Path(__file__).resolve().parents[1] / "dbt/seeds/reference/exception_types.csv"
T0 = datetime(2026, 1, 1, 12, 0, 0)


def alerts(spark, *types):
    cols = (
        "exception_id string, booking_id string, exception_type string, "
        "expected_amount_cents long, actual_amount_cents long, amount_at_risk_cents long, "
        "booking_currency string, payment_currency string, payment_ids string, "
        "due_at timestamp, evaluated_at timestamp, detected_by string"
    )
    rows = [
        (f"x{i}", f"b{i}", t, 1000, 2000, 1000, "USD", "USD", "p1", T0, T0, "stream")
        for i, t in enumerate(types)
    ]
    return spark.createDataFrame(rows, cols)


def test_severity_map_matches_the_dbt_seed():
    with SEED.open() as f:
        seed = {r["exception_type"]: r["severity"] for r in csv.DictReader(f)}
    assert EXCEPTION_SEVERITY == seed
    assert set(EXCEPTION_TYPES) == set(seed), "every exception type needs a severity"


def test_alert_message_carries_severity_from_the_seed(spark):
    msgs = alert_messages(alerts(spark, "DUPLICATE_CHARGE", "ORPHAN_PAYMENT")).collect()
    by_type = {json.loads(m.value)["exception_type"]: json.loads(m.value) for m in msgs}
    assert by_type["DUPLICATE_CHARGE"]["severity"] == "critical"
    assert by_type["ORPHAN_PAYMENT"]["severity"] == "medium"


def test_alert_message_keeps_existing_fields_and_key(spark):
    [m] = alert_messages(alerts(spark, "MISSING_PAYMENT")).collect()
    payload = json.loads(m.value)
    assert m.key == "b0"
    assert list(payload) == ALERT_COLUMNS
    assert set(EXCEPTION_COLUMNS) <= set(payload)
    assert payload["severity"] == "high"


def test_unmapped_type_still_alerts_as_unknown(spark):
    [m] = alert_messages(alerts(spark, "SOMETHING_NEW")).collect()
    assert json.loads(m.value)["severity"] == UNKNOWN_SEVERITY
