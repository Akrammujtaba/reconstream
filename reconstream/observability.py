"""Metrics for Prometheus (via Pushgateway) and alerts for new exceptions."""

from __future__ import annotations

import json
import logging
import urllib.request
from collections import defaultdict

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

log = logging.getLogger("reconstream.observability")


class StreamMetrics:
    """Holds cumulative counters on the driver and pushes a snapshot after each batch."""

    def __init__(self, gateway: str | None, job: str = "reconstream_stream"):
        self.gateway = gateway
        self.job = job
        self.totals: dict[str, float] = defaultdict(float)
        # (exception_type, "opened" | "resolved") -> cumulative count since start.
        self.changes_by_type: dict[tuple[str, str], float] = defaultdict(float)

    def push(self, batch: dict, open_by_type: dict[str, tuple[int, int]],
             changes_by_type: dict[tuple[str, str], int] | None = None) -> None:
        for key in ("bookings_events", "payment_events", "duplicate_deliveries",
                    "new_exceptions", "resolved_exceptions"):
            self.totals[key] += batch.get(key, 0)
        for key, n in (changes_by_type or {}).items():
            self.changes_by_type[key] += n
        if not self.gateway:
            return
        try:
            push_to_gateway(self.gateway, job=self.job,
                            registry=self.registry(batch, open_by_type))
        except Exception as exc:  # metrics must never take the pipeline down
            log.warning("pushgateway unavailable: %s", exc)

    def registry(self, batch: dict, open_by_type: dict[str, tuple[int, int]]) -> CollectorRegistry:
        """Build the snapshot that is pushed after a batch.

        Separate from `push` so tests (and the dashboard check) can read the
        exact metric names and values without a Pushgateway.
        """
        reg = CollectorRegistry()
        Gauge("reconstream_batch_duration_seconds", "Last micro-batch duration",
              registry=reg).set(batch.get("duration_s", 0))
        Gauge("reconstream_batch_input_rows", "Rows in last micro-batch",
              registry=reg).set(batch.get("input_rows", 0))
        Gauge("reconstream_watermark_lag_seconds",
              "Wall clock minus event-time watermark", registry=reg).set(
            batch.get("watermark_lag_s", 0)
        )
        Gauge("reconstream_last_batch_timestamp_seconds", "Unix time of last batch",
              registry=reg).set_to_current_time()
        totals = Gauge("reconstream_events_total", "Cumulative events by kind",
                       ["kind"], registry=reg)
        for key, value in self.totals.items():
            totals.labels(kind=key).set(value)
        open_g = Gauge("reconstream_open_exceptions", "Open exceptions by type",
                       ["exception_type"], registry=reg)
        risk_g = Gauge("reconstream_amount_at_risk_cents", "Open amount at risk by type",
                       ["exception_type"], registry=reg)
        for etype, (count, risk) in open_by_type.items():
            open_g.labels(exception_type=etype).set(count)
            risk_g.labels(exception_type=etype).set(risk)
        # A Gauge holding a running total, like reconstream_events_total: the
        # Pushgateway keeps whatever value was pushed last, and PromQL rate()
        # treats a drop after a restart as a counter reset.
        changes_g = Gauge("reconstream_exception_changes_total",
                          "Cumulative exceptions opened or resolved, by type",
                          ["exception_type", "change"], registry=reg)
        for (etype, change), value in self.changes_by_type.items():
            changes_g.labels(exception_type=etype, change=change).set(value)
        return reg


def post_slack_summary(webhook_url: str | None, alerts: list[dict]) -> None:
    if not webhook_url or not alerts:
        return
    lines = [
        f"• {a['exception_type']} on {a['booking_id']} "
        f"(at risk: {a['amount_at_risk_cents'] / 100:.2f} {a.get('booking_currency') or ''})"
        for a in alerts[:10]
    ]
    more = f"\n…and {len(alerts) - 10} more" if len(alerts) > 10 else ""
    body = json.dumps({"text": f"ReconStream: {len(alerts)} new exception(s)\n"
                               + "\n".join(lines) + more}).encode()
    req = urllib.request.Request(webhook_url, data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=5).read()
    except Exception as exc:
        log.warning("slack alert failed: %s", exc)
