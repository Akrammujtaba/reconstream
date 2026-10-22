"""Run the simulator against the real stack: bookings to Postgres, payments to the webhook.

    python run.py --rate 5 --duration 1800
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import os
import signal
import time
from datetime import datetime, timedelta, timezone

import httpx
import psycopg2
from psycopg2.extras import Json
from simulator import Config, Simulator

log = logging.getLogger("generator")


class LiveSink:
    def __init__(self, dsn: str, webhook_url: str, secret: str):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True
        self.http = httpx.Client(timeout=10)
        self.url = webhook_url
        self.secret = secret.encode()
        self.stats = {"accepted": 0, "rejected": 0, "retried": 0, "failed": 0}

    def insert_booking(self, row: dict) -> None:
        cols = ", ".join(row)
        vals = ", ".join(["%s"] * len(row))
        with self.conn.cursor() as cur:
            cur.execute(f"INSERT INTO public.bookings ({cols}) VALUES ({vals})", list(row.values()))

    def update_booking(self, booking_id: str, changes: dict) -> None:
        sets = ", ".join(f"{k} = %s" for k in changes)
        with self.conn.cursor() as cur:
            cur.execute(f"UPDATE public.bookings SET {sets} WHERE booking_id = %s",
                        [*changes.values(), booking_id])

    def record_fault(self, fault: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO audit.injected_faults
                   (booking_id, fault_type, expected_exception, expect_resolved,
                    injected_at, detectable_at, details)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                [fault["booking_id"], fault["fault_type"], fault["expected_exception"],
                 fault["expect_resolved"], fault["injected_at"], fault["detectable_at"],
                 Json(fault["details"])],
            )

    def deliver(self, event: dict) -> None:
        body = json.dumps(event).encode()
        sig = "sha256=" + hmac.new(self.secret, body, hashlib.sha256).hexdigest()
        for attempt in range(4):  # providers retry 5xx with backoff
            try:
                r = self.http.post(self.url, content=body,
                                   headers={"X-Signature": sig, "Content-Type": "application/json"})
            except httpx.HTTPError as exc:
                log.warning("webhook unreachable: %s", exc)
                r = None
            if r is not None and r.status_code < 500:
                self.stats["accepted" if r.status_code == 202 else "rejected"] += 1
                return
            self.stats["retried"] += 1
            time.sleep(0.5 * 2 ** attempt)
        self.stats["failed"] += 1


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is noise
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=float(os.environ.get("GEN_RATE", "2")),
                   help="new bookings per second")
    p.add_argument("--duration", type=int, default=int(os.environ.get("GEN_DURATION", "0")),
                   help="seconds to run; 0 = until stopped")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--fault-scale", type=float, default=1.0,
                   help="multiply every fault rate, e.g. 2 for a noisier demo")
    args = p.parse_args()

    sla_minutes = int(os.environ.get("RECON_SLA_MINUTES", "2"))
    cfg = Config(bookings_per_second=args.rate, sla=timedelta(minutes=sla_minutes))
    cfg.fault_rates = {k: v * args.fault_scale for k, v in cfg.fault_rates.items()}
    cfg.orphan_rate *= args.fault_scale

    sink = LiveSink(
        os.environ.get("BOOKINGS_DSN", "postgresql://recon:recon@postgres:5432/bookings"),
        os.environ.get("WEBHOOK_URL", "http://webhook:8000/webhooks/payments"),
        os.environ.get("WEBHOOK_SECRET", "dev-secret"),
    )
    sim = Simulator(sink, cfg, seed=args.seed)

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    started = time.time()
    last_log = started
    log.info("generating %.1f bookings/s, SLA %d min", args.rate, sla_minutes)
    while not stopping and (args.duration == 0 or time.time() - started < args.duration):
        sim.tick(datetime.now(timezone.utc))
        if time.time() - last_log > 30:
            log.info("counts=%s webhook=%s", sim.counts, sink.stats)
            last_log = time.time()
        time.sleep(0.05)

    log.info("stopping: delivering events already in flight")
    while sim._queue:
        sim.tick(datetime.now(timezone.utc))
        time.sleep(0.1)
    log.info("final counts=%s webhook=%s", sim.counts, sink.stats)


if __name__ == "__main__":
    main()
