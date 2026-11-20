"""Replay dead-lettered payment webhooks after fixing a known class of error.

The webhook sends every payload that breaks the contract to `payments.dlq` with
the reason and the per-field errors. Most of those are genuinely bad and should
stay there. A few are a provider formatting slip where the intent is obvious,
and dropping them would leave a real payment out of reconciliation (it would
show up later as a false MISSING_PAYMENT).

This tool reads the DLQ, applies a fixer only when a record's errors match that
fixer exactly, checks the fixed payload against the same contract locally, and
re-posts it to the webhook with a fresh signature. It does not write to Kafka
directly: the fixed event goes through the normal front door, so it gets the
same validation, the future-date check, and the same `received_at` stamp.

Re-running is safe. Silver payments MERGE insert-only on event_id, so an event
that was already replayed is ignored downstream.

Dry run is the default; nothing is posted without --apply.

    python replay_dlq.py --bootstrap kafka:9092 --webhook http://webhook:8000/webhooks/payments
    python replay_dlq.py --from-file dlq.jsonl --fix lowercase_currency --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field

from app import PaymentEvent, sign
from pydantic import ValidationError

log = logging.getLogger("replay_dlq")

# A fixer takes the parsed payload and returns a corrected copy, or None if the
# payload doesn't look the way that fixer expects. It only ever sees records whose
# DLQ errors are all on the fields it owns (see FIXERS below).
Fixer = Callable[[dict], "dict | None"]

_ALLOWED_CURRENCIES = {"USD", "EUR", "GBP", "INR"}
# ISO-8601 date-time with no offset and no trailing Z, e.g. 2026-01-01T12:00:00.123
_NAIVE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?$")


def fix_lowercase_currency(payload: dict) -> dict | None:
    """'usd' -> 'USD'. Only when the upper-cased code is one we accept, so a real
    unknown currency (e.g. 'xyz') stays in the DLQ."""
    cur = payload.get("currency")
    if not isinstance(cur, str) or cur.upper() not in _ALLOWED_CURRENCIES or cur.isupper():
        return None
    return {**payload, "currency": cur.upper()}


def fix_naive_utc_timestamp(payload: dict) -> dict | None:
    """'2026-01-01T12:00:00' -> '2026-01-01T12:00:00+00:00'.

    The contract rejects timestamps without an offset because event time drives
    the watermark, and a guessed zone could shift it by hours. This fixer is for
    the case where you have confirmed with the provider that those timestamps
    were UTC. It is opt-in for that reason (see DEFAULT_FIXES)."""
    ts = payload.get("occurred_at")
    if not isinstance(ts, str) or not _NAIVE_ISO.match(ts):
        return None
    return {**payload, "occurred_at": ts + "+00:00"}


@dataclass(frozen=True)
class FixSpec:
    fields: frozenset[str]  # the record's errors must all be on these fields
    fix: Fixer


FIXERS: dict[str, FixSpec] = {
    "lowercase_currency": FixSpec(frozenset({"currency"}), fix_lowercase_currency),
    "naive_utc_timestamp": FixSpec(frozenset({"occurred_at"}), fix_naive_utc_timestamp),
}
# Safe to run without asking anyone: the intent is unambiguous.
DEFAULT_FIXES = ("lowercase_currency",)


@dataclass
class Decision:
    """What the tool decided for one DLQ record."""

    outcome: str  # "fixed" or "skipped"
    why: str  # fixer name, or the reason it was left in the DLQ
    payload: dict | None = None  # the corrected event, when fixed


def plan(record: dict, fixes: Iterable[str]) -> Decision:
    """Decide whether one DLQ record can be fixed. Pure, so it is easy to test.

    Only contract violations are candidates. invalid_json has nothing safe to
    repair, and occurred_in_future is a clock problem on the sender's side that a
    replay would just reject again."""
    reason = record.get("reason")
    if reason != "contract_violation":
        return Decision("skipped", f"reason:{reason}")
    detail = record.get("detail") or []
    error_fields = {d.get("field") for d in detail if isinstance(d, dict)}
    if not error_fields:
        return Decision("skipped", "no_detail")
    try:
        payload = json.loads(record.get("payload", ""))
    except ValueError:
        return Decision("skipped", "unparseable_payload")
    if not isinstance(payload, dict):
        return Decision("skipped", "unparseable_payload")

    for name in fixes:
        spec = FIXERS[name]
        # Every error must belong to this fixer. A record with a lowercase currency
        # AND a negative amount is still bad after the fix, so leave it alone.
        if not error_fields <= spec.fields:
            continue
        fixed = spec.fix(payload)
        if fixed is None:
            continue
        try:
            PaymentEvent.model_validate(fixed)
        except ValidationError:
            return Decision("skipped", f"still_invalid_after:{name}")
        return Decision("fixed", name, fixed)
    return Decision("skipped", "no_matching_fixer")


@dataclass
class Report:
    seen: int = 0
    by_outcome: Counter = field(default_factory=Counter)
    posted: Counter = field(default_factory=Counter)  # HTTP status -> count

    def summary(self) -> dict:
        return {"seen": self.seen, "decisions": dict(self.by_outcome),
                "posted": {str(k): v for k, v in self.posted.items()}}


def replay(records: Iterable[dict], fixes: Iterable[str], post: Callable[[bytes, str], int] | None,
           secret: str) -> Report:
    """Plan every record and, if `post` is given, re-send the fixed ones.

    `post(body, signature)` returns the webhook's HTTP status. 202 means it is back
    in payments.raw; 400 means the webhook still disagrees (it will have written a
    fresh DLQ record with the new reason)."""
    fixes = tuple(fixes)
    report = Report()
    for record in records:
        report.seen += 1
        decision = plan(record, fixes)
        report.by_outcome[f"{decision.outcome}:{decision.why}"] += 1
        if decision.outcome != "fixed" or post is None:
            continue
        # Signed exactly like the provider does, so the webhook needs no bypass.
        body = json.dumps(decision.payload).encode()
        report.posted[post(body, sign(secret, body))] += 1
    return report


def read_jsonl(path: str) -> Iterator[dict]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def read_kafka(bootstrap: str, topic: str) -> Iterator[dict]:  # pragma: no cover - needs a broker
    """Read the topic from the start up to the end offsets as of now, then stop.

    No consumer group commits: the tool is a one-off, and replaying twice is
    harmless because of event_id idempotency."""
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "dlq-replay",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
        remaining = {p for p, end in ends.items() if end > 0}
        c.assign([TopicPartition(topic, p, 0) for p in remaining])
        while remaining:
            msg = c.poll(5)
            if msg is None:
                break
            if msg.error():
                raise RuntimeError(msg.error())
            if msg.offset() >= ends[msg.partition()] - 1:
                remaining.discard(msg.partition())
            yield json.loads(msg.value())
    finally:
        c.close()


def main(argv: list[str] | None = None) -> dict:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--bootstrap", help="Kafka bootstrap servers to read the DLQ from")
    src.add_argument("--from-file", help="JSONL file of DLQ records (e.g. exported with kcat)")
    ap.add_argument("--topic", default=os.environ.get("DLQ_TOPIC", "payments.dlq"))
    ap.add_argument("--webhook", default="http://localhost:8000/webhooks/payments")
    ap.add_argument("--fix", action="append", choices=sorted(FIXERS),
                    help=f"fixer to apply; repeatable (default: {', '.join(DEFAULT_FIXES)})")
    ap.add_argument("--apply", action="store_true", help="actually post; default is a dry run")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    records = read_jsonl(args.from_file) if args.from_file else read_kafka(args.bootstrap, args.topic)
    secret = os.environ.get("WEBHOOK_SECRET", "dev-secret")

    post = None
    if args.apply:
        import httpx

        client = httpx.Client(timeout=10)

        def post(body: bytes, signature: str) -> int:
            return client.post(args.webhook, content=body, headers={
                "X-Signature": signature, "Content-Type": "application/json"}).status_code

    summary = replay(records, args.fix or DEFAULT_FIXES, post, secret).summary()
    summary["mode"] = "apply" if args.apply else "dry-run"
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    main()
