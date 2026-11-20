"""DLQ replay: DLQ records are produced by the real webhook app, then replayed
back into it, so the test covers the exact record shape the webhook writes."""

import json
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "webhook"))
from app import create_app, sign  # noqa: E402
from replay_dlq import main, plan, replay  # noqa: E402
from test_webhook import FakeProducer, event  # noqa: E402

SECRET = "test-secret"


@pytest.fixture
def webhook():
    producer = FakeProducer()
    return TestClient(create_app(producer, SECRET)), producer


def send(client, payload=None, raw=None):
    body = raw if raw is not None else json.dumps(payload).encode()
    return client.post("/webhooks/payments", content=body,
                       headers={"X-Signature": sign(SECRET, body)})


def dlq_records(client, producer, *payloads):
    """Push payloads through the webhook and return what it dead-lettered."""
    for p in payloads:
        if isinstance(p, bytes):
            send(client, raw=p)
        else:
            send(client, p)
    return [v for t, _, v in producer.sent if t == "payments.dlq"]


def poster(client):
    def post(body, signature):
        return client.post("/webhooks/payments", content=body,
                           headers={"X-Signature": signature}).status_code
    return post


def test_lowercase_currency_is_fixed_and_lands_in_payments_raw(webhook):
    client, producer = webhook
    [record] = dlq_records(client, producer, event(event_id="evt_lc", currency="usd"))

    report = replay([record], ["lowercase_currency"], poster(client), SECRET)

    assert report.posted == {202: 1}
    raw = [v for t, _, v in producer.sent if t == "payments.raw"]
    assert [(v["event_id"], v["currency"]) for v in raw] == [("evt_lc", "USD")]


def test_naive_timestamp_is_read_as_utc_only_when_asked(webhook):
    client, producer = webhook
    [record] = dlq_records(client, producer, event(occurred_at="2026-01-01T12:00:00"))

    # Not in the defaults: guessing a zone moves event time, so it's opt-in.
    assert plan(record, ["lowercase_currency"]).why == "no_matching_fixer"

    decision = plan(record, ["naive_utc_timestamp"])
    assert decision.outcome == "fixed"
    assert decision.payload["occurred_at"] == "2026-01-01T12:00:00+00:00"


@pytest.mark.parametrize("bad, why", [
    # Unknown currency even after upper-casing: a real error, not formatting.
    (event(currency="xyz"), "no_matching_fixer"),
    # Fixable currency, but the amount is also wrong; one fix wouldn't make it valid.
    (event(currency="usd", amount_cents=-1), "no_matching_fixer"),
    (event(amount_cents=-1), "no_matching_fixer"),
    (b"{not json", "reason:invalid_json"),
])
def test_records_that_are_really_bad_stay_in_the_dlq(webhook, bad, why):
    client, producer = webhook
    [record] = dlq_records(client, producer, bad)
    assert plan(record, ["lowercase_currency", "naive_utc_timestamp"]).why == why


def test_future_dated_events_are_never_replayed(webhook):
    from datetime import datetime, timedelta, timezone
    client, producer = webhook
    future = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    [record] = dlq_records(client, producer, event(occurred_at=future))
    assert plan(record, ["lowercase_currency"]).why == "reason:occurred_in_future"


def test_dry_run_posts_nothing_but_reports_what_it_would_fix(webhook):
    client, producer = webhook
    records = dlq_records(client, producer,
                          event(event_id="a", currency="eur"),
                          event(event_id="b", currency="xyz"))
    before = len(producer.sent)

    report = replay(records, ["lowercase_currency"], post=None, secret=SECRET)

    assert len(producer.sent) == before
    assert report.seen == 2
    assert report.by_outcome == {"fixed:lowercase_currency": 1,
                                 "skipped:no_matching_fixer": 1}


def test_replaying_twice_sends_the_same_event_id(webhook):
    """The second replay is accepted by the webhook too; idempotency comes from
    silver.payments merging on event_id, so both copies must keep the original id."""
    client, producer = webhook
    [record] = dlq_records(client, producer, event(event_id="evt_twice", currency="gbp"))
    replay([record], ["lowercase_currency"], poster(client), SECRET)
    replay([record], ["lowercase_currency"], poster(client), SECRET)
    raw = [v["event_id"] for t, _, v in producer.sent if t == "payments.raw"]
    assert raw == ["evt_twice", "evt_twice"]


def test_cli_dry_run_from_file(webhook, tmp_path, capsys):
    client, producer = webhook
    records = dlq_records(client, producer, event(currency="inr"), event(amount_cents=-5))
    path = tmp_path / "dlq.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")

    summary = main(["--from-file", str(path)])

    assert summary["mode"] == "dry-run"
    assert summary["decisions"] == {"fixed:lowercase_currency": 1,
                                    "skipped:no_matching_fixer": 1}
    assert summary["posted"] == {}
    assert json.loads(capsys.readouterr().out) == summary
