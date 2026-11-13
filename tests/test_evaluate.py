import csv
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "evaluation"))
from evaluate import (  # noqa: E402
    CSV_FIELDS,
    Found,
    Truth,
    append_csv,
    compute_report,
    percentile,
    to_csv_rows,
    to_markdown,
)

NOW = datetime(2026, 1, 1, 13, 0, tzinfo=timezone.utc)
T = NOW - timedelta(minutes=30)


def test_percentile():
    assert percentile([], 95) is None
    assert percentile([10.0], 95) == 10.0
    assert percentile([0, 10, 20, 30, 40], 50) == 20


def test_report_scores_recall_precision_latency_and_late_payments():
    truth = [
        Truth("b1", "DUPLICATE_CHARGE", "DUPLICATE_CHARGE", False, T),
        Truth("b2", "MISSING_PAYMENT", "MISSING_PAYMENT", False, T),
        Truth("b3", "LATE_PAYMENT", "MISSING_PAYMENT", True, T),
        # Too recent to score yet:
        Truth("b4", "AMOUNT_MISMATCH", "AMOUNT_MISMATCH", False, NOW - timedelta(minutes=1)),
    ]
    found = [
        Found("b1", "DUPLICATE_CHARGE", "OPEN", T + timedelta(seconds=40), T),
        Found("b3", "MISSING_PAYMENT", "RESOLVED", T + timedelta(seconds=50), T),
        Found("bX", "AMOUNT_MISMATCH", "OPEN", T, T),  # false positive
    ]
    r = compute_report(truth, found, NOW)
    assert (r["injected"], r["detected"]) == (2, 1)
    assert r["recall"] == 0.5
    assert r["false_positives"] == 1
    assert r["precision"] == 0.5
    assert r["late_payments_auto_resolved"] == (1, 1)
    assert r["latency_p50_s"] == 40
    md = to_markdown(r)
    assert "| Recall | 50.0% |" in md


def test_since_window_excludes_older_faults():
    truth = [
        Truth("old", "DUPLICATE_CHARGE", "DUPLICATE_CHARGE", False, T - timedelta(hours=1)),
        Truth("new", "DUPLICATE_CHARGE", "DUPLICATE_CHARGE", False, T),
    ]
    found = [Found("new", "DUPLICATE_CHARGE", "OPEN", T + timedelta(seconds=20), T)]
    r = compute_report(truth, found, NOW, since=T - timedelta(minutes=5))
    assert (r["injected"], r["detected"], r["latency_p50_s"]) == (1, 1, 20)


def _report():
    truth = [
        Truth("b1", "DUPLICATE_CHARGE", "DUPLICATE_CHARGE", False, T),
        Truth("b2", "MISSING_PAYMENT", "MISSING_PAYMENT", False, T),
    ]
    found = [
        Found("b1", "DUPLICATE_CHARGE", "OPEN", T + timedelta(seconds=40), T),
        Found("bX", "AMOUNT_MISMATCH", "OPEN", T, T),  # false positive
    ]
    return compute_report(truth, found, NOW)


def test_csv_rows_have_overall_row_then_one_per_type_with_raw_numbers():
    rows = to_csv_rows(_report())
    assert [r["fault_type"] for r in rows] == ["ALL", "DUPLICATE_CHARGE", "MISSING_PAYMENT"]
    overall, dup, missing = rows
    assert overall["recall"] == 0.5 and overall["precision"] == 0.5
    assert overall["false_positives"] == 1
    assert overall["evaluated_at"] == NOW.isoformat() and overall["since"] == ""
    assert (dup["detected"], dup["latency_p50_s"]) == (1, 40)
    # Precision is not defined per type, and an undetected type has no latency.
    assert dup["precision"] == "" and dup["false_positives"] == ""
    assert (missing["recall"], missing["latency_p50_s"]) == (0.0, "")


def test_append_csv_writes_header_once_and_keeps_history(tmp_path):
    path = tmp_path / "history.csv"
    append_csv(_report(), str(path))
    append_csv(_report(), str(path))
    with open(path, newline="") as f:
        lines = f.read().splitlines()
    assert lines[0] == ",".join(CSV_FIELDS)
    assert sum(line == lines[0] for line in lines) == 1
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 6  # 3 rows per run, two runs
    assert rows[0]["fault_type"] == "ALL" and float(rows[0]["recall"]) == 0.5
