"""In-memory stand-in for IcebergStore with the same MERGE semantics, for tests."""

from __future__ import annotations

BOOKINGS = (
    "booking_id string, guest_id string, listing_id string, status string, amount_cents long, "
    "currency string, created_at timestamp, confirmed_at timestamp, cancelled_at timestamp, "
    "updated_at timestamp, is_deleted boolean, source_lsn long, cdc_op string, "
    "ingested_at timestamp"
)
PAYMENTS = (
    "event_id string, event_type string, payment_id string, booking_id string, "
    "amount_cents long, currency string, occurred_at timestamp, received_at timestamp, "
    "kafka_ts timestamp, ingested_at timestamp"
)
EXCEPTIONS = (
    "exception_id string, booking_id string, exception_type string, status string, "
    "expected_amount_cents long, actual_amount_cents long, amount_at_risk_cents long, "
    "booking_currency string, payment_currency string, payment_ids string, due_at timestamp, "
    "first_detected_at timestamp, last_seen_at timestamp, last_evaluated_at timestamp, "
    "resolved_at timestamp, detected_by string, reopen_count int"
)

_FIELDS = {
    name: [part.strip().split(" ")[0] for part in schema.split(",")]
    for name, schema in (("b", BOOKINGS), ("p", PAYMENTS), ("e", EXCEPTIONS))
}


class MemoryStore:
    def __init__(self, spark):
        self.spark = spark
        self.b: dict[str, dict] = {}
        self.p: dict[str, dict] = {}
        self.e: dict[str, dict] = {}
        self.bronze_rows = 0
        self.alerts: list[dict] = []

    def _df(self, rows: dict, key: str, schema: str):
        fields = _FIELDS[key]
        return self.spark.createDataFrame(
            [tuple(r.get(f) for f in fields) for r in rows.values()], schema
        )

    def bookings(self):
        return self._df(self.b, "b", BOOKINGS)

    def payments(self):
        return self._df(self.p, "p", PAYMENTS)

    def exceptions(self):
        return self._df(self.e, "e", EXCEPTIONS)

    def append_bronze(self, raw, batch_id):
        self.bronze_rows += raw.count()

    def merge_bookings(self, latest, created_range=None):
        for r in latest.collect():
            cur = self.b.get(r.booking_id)
            if cur is None or cur["source_lsn"] is None or r.source_lsn > cur["source_lsn"]:
                self.b[r.booking_id] = r.asDict()

    def merge_payments(self, events, occurred_range=None):
        for r in events.collect():
            self.p.setdefault(r.event_id, r.asDict())

    def merge_exceptions(self, changes):
        for s in changes.collect():
            t = self.e.get(s.exception_id)
            if t is not None and s.change == "RESOLVE" and t["status"] == "OPEN":
                t.update(status="RESOLVED", resolved_at=s.evaluated_at,
                         last_evaluated_at=s.evaluated_at)
            elif t is not None and s.change == "UPSERT":
                t.update(
                    reopen_count=t["reopen_count"] + (1 if t["status"] == "RESOLVED" else 0),
                    status="OPEN", resolved_at=None,
                    expected_amount_cents=s.expected_amount_cents,
                    actual_amount_cents=s.actual_amount_cents,
                    amount_at_risk_cents=s.amount_at_risk_cents,
                    payment_currency=s.payment_currency, payment_ids=s.payment_ids,
                    due_at=s.due_at, last_seen_at=s.evaluated_at,
                    last_evaluated_at=s.evaluated_at,
                )
            elif t is None and s.change == "UPSERT":
                row = {f: getattr(s, f, None) for f in _FIELDS["e"]}
                row.update(status="OPEN", first_detected_at=s.evaluated_at,
                           last_seen_at=s.evaluated_at, last_evaluated_at=s.evaluated_at,
                           resolved_at=None, reopen_count=0)
                self.e[s.exception_id] = row

    def file_stats(self):
        # Tests set this to whatever snapshot summaries they want the processor to see.
        return getattr(self, "snapshots", {})

    def maintain(self, older_than):
        self.maintenance_runs = getattr(self, "maintenance_runs", 0) + 1

    def publish_alerts(self, alerts):
        self.alerts.extend(r.asDict() for r in alerts.collect())
