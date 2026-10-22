"""Measure the live pipeline against the generator's ground truth.

Compares audit.injected_faults (what was broken on purpose) with
gold.recon_exceptions (what the pipeline found) and reports, per fault type:
  recall      share of injected faults that were detected
  precision   share of open exceptions that correspond to a real fault
  latency     first_detected_at - detectable_at (p50 / p95)

    python evaluation/evaluate.py            # prints a table, writes evaluation/results.md
"""

from __future__ import annotations

import argparse
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass
class Truth:
    booking_id: str
    fault_type: str
    expected_exception: str
    expect_resolved: bool
    detectable_at: datetime


@dataclass
class Found:
    booking_id: str
    exception_type: str
    status: str
    first_detected_at: datetime
    due_at: datetime | None


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def compute_report(truth: list[Truth], found: list[Found], now: datetime,
                   grace: timedelta = timedelta(minutes=3)) -> dict:
    """Only faults that became detectable at least `grace` ago are scored."""
    cutoff = now - grace
    found_by_key = {(f.booking_id, f.exception_type): f for f in found}
    scored = [t for t in truth if t.detectable_at <= cutoff]

    per_type: dict[str, dict] = defaultdict(lambda: {"injected": 0, "detected": 0, "latencies": []})
    late_ok = late_total = 0
    for t in scored:
        f = found_by_key.get((t.booking_id, t.expected_exception))
        if t.expect_resolved:
            late_total += 1
            if f is None or f.status == "RESOLVED":
                late_ok += 1
            continue
        row = per_type[t.fault_type]
        row["injected"] += 1
        if f is not None:
            row["detected"] += 1
            row["latencies"].append((f.first_detected_at - t.detectable_at).total_seconds())

    # False positives: open exceptions with no matching injected fault.
    expected_keys = {(t.booking_id, t.expected_exception) for t in truth}
    open_scored = [f for f in found if f.status == "OPEN"
                   and (f.due_at is None or f.due_at <= cutoff)]
    false_pos = [f for f in open_scored if (f.booking_id, f.exception_type) not in expected_keys]

    all_lat = [x for r in per_type.values() for x in r["latencies"]]
    injected = sum(r["injected"] for r in per_type.values())
    detected = sum(r["detected"] for r in per_type.values())
    return {
        "now": now,
        "injected": injected,
        "detected": detected,
        "recall": detected / injected if injected else None,
        "precision": (len(open_scored) - len(false_pos)) / len(open_scored) if open_scored else None,
        "false_positives": len(false_pos),
        "late_payments_auto_resolved": (late_ok, late_total),
        "latency_p50_s": percentile(all_lat, 50),
        "latency_p95_s": percentile(all_lat, 95),
        "per_type": {
            k: {
                "injected": v["injected"],
                "detected": v["detected"],
                "recall": v["detected"] / v["injected"] if v["injected"] else None,
                "p50_s": percentile(v["latencies"], 50),
                "p95_s": percentile(v["latencies"], 95),
            }
            for k, v in sorted(per_type.items())
        },
    }


def _fmt(x, pct=False, secs=False):
    if x is None:
        return "–"
    if pct:
        return f"{x * 100:.1f}%"
    if secs:
        return f"{x:.0f}s"
    return str(x)


def to_markdown(r: dict, throughput: str | None = None) -> str:
    late_ok, late_total = r["late_payments_auto_resolved"]
    lines = [
        f"# ReconStream evaluation ({r['now']:%Y-%m-%d %H:%M} UTC)",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Injected faults scored | {r['injected']} |",
        f"| Detected | {r['detected']} |",
        f"| Recall | {_fmt(r['recall'], pct=True)} |",
        f"| Precision (open exceptions) | {_fmt(r['precision'], pct=True)} |",
        f"| False positives | {r['false_positives']} |",
        f"| Detection latency p50 | {_fmt(r['latency_p50_s'], secs=True)} |",
        f"| Detection latency p95 | {_fmt(r['latency_p95_s'], secs=True)} |",
        f"| Late payments auto-resolved | {late_ok}/{late_total} |",
    ]
    if throughput:
        lines.append(f"| Throughput | {throughput} |")
    lines += ["", "| Fault type | Injected | Detected | Recall | p50 | p95 |", "|---|---|---|---|---|---|"]
    for k, v in r["per_type"].items():
        lines.append(
            f"| {k} | {v['injected']} | {v['detected']} | {_fmt(v['recall'], pct=True)} "
            f"| {_fmt(v['p50_s'], secs=True)} | {_fmt(v['p95_s'], secs=True)} |"
        )
    lines += ["", "Latency = time from when a fault became detectable (payment arrived, or "
              "SLA deadline passed) to when the pipeline first recorded it."]
    return "\n".join(lines) + "\n"


def _aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def main() -> None:
    import psycopg2
    import trino

    p = argparse.ArgumentParser()
    p.add_argument("--pg", default=os.environ.get(
        "BOOKINGS_DSN", "postgresql://recon:recon@localhost:5432/bookings"))
    p.add_argument("--trino-host", default=os.environ.get("TRINO_HOST", "localhost"))
    p.add_argument("--trino-port", type=int, default=int(os.environ.get("TRINO_PORT", "8080")))
    p.add_argument("--grace-minutes", type=float, default=3)
    p.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "results.md"))
    args = p.parse_args()

    with psycopg2.connect(args.pg) as conn, conn.cursor() as cur:
        cur.execute("""SELECT booking_id, fault_type, expected_exception, expect_resolved,
                              detectable_at FROM audit.injected_faults""")
        truth = [Truth(r[0], r[1], r[2], r[3], _aware(r[4])) for r in cur.fetchall()]

    tconn = trino.dbapi.connect(host=args.trino_host, port=args.trino_port, user="evaluator",
                                catalog="lake", schema="gold")
    tcur = tconn.cursor()
    tcur.execute("""SELECT booking_id, exception_type, status, first_detected_at, due_at
                    FROM recon_exceptions""")
    found = [Found(r[0], r[1], r[2], _aware(r[3]), _aware(r[4])) for r in tcur.fetchall()]

    tcur.execute("""SELECT count(*), min(occurred_at), max(occurred_at) FROM silver.payments""")
    n, lo, hi = tcur.fetchone()
    throughput = None
    if n and lo and hi and hi > lo:
        throughput = f"{n / (hi - lo).total_seconds():.1f} payment events/sec sustained ({n:,} total)"

    report = compute_report(truth, found, datetime.now(timezone.utc),
                            timedelta(minutes=args.grace_minutes))
    md = to_markdown(report, throughput)
    print(md)
    with open(args.out, "w") as f:
        f.write(md)


if __name__ == "__main__":
    main()
