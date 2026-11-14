import json
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "webhook"))
from app import HeartbeatEmitter, create_app, heartbeat_records, sign  # noqa: E402

SECRET = "test-secret"


class FakeProducer:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    def send(self, topic, key, value):
        if self.fail and topic == "payments.raw":
            raise RuntimeError("broker down")
        self.sent.append((topic, key, value))


def event(**over):
    e = {
        "event_id": "evt_1", "type": "payment.captured", "payment_id": "pay_1",
        "booking_id": "bk_1", "amount_cents": 12500, "currency": "USD",
        "occurred_at": "2026-01-01T12:00:00Z",
    }
    e.update(over)
    return e


@pytest.fixture
def client_and_producer():
    producer = FakeProducer()
    return TestClient(create_app(producer, SECRET)), producer


def post(client, payload, secret=SECRET, raw=None):
    body = raw if raw is not None else json.dumps(payload).encode()
    return client.post("/webhooks/payments", content=body,
                       headers={"X-Signature": sign(secret, body)})


def test_valid_event_is_published_keyed_by_booking(client_and_producer):
    client, producer = client_and_producer
    r = post(client, event())
    assert r.status_code == 202
    topic, key, value = producer.sent[0]
    assert (topic, key) == ("payments.raw", "bk_1")
    assert value["amount_cents"] == 12500 and "received_at" in value


def test_bad_signature_is_rejected_without_publishing(client_and_producer):
    client, producer = client_and_producer
    assert post(client, event(), secret="wrong").status_code == 401
    assert producer.sent == []


@pytest.mark.parametrize("bad", [
    event(amount_cents=-1),
    event(currency="XYZ"),
    event(type="charge.unknown"),
    event(occurred_at="2026-01-01T12:00:00"),  # no timezone
    {k: v for k, v in event().items() if k != "booking_id"},
    event(surprise="field"),
])
def test_contract_violations_go_to_dlq(client_and_producer, bad):
    client, producer = client_and_producer
    r = post(client, bad)
    assert r.status_code == 400
    assert [t for t, _, _ in producer.sent] == ["payments.dlq"]
    assert producer.sent[0][2]["reason"] == "contract_violation"


def test_invalid_json_goes_to_dlq(client_and_producer):
    client, producer = client_and_producer
    r = post(client, None, raw=b"{not json")
    assert r.status_code == 400
    assert producer.sent[0][2]["reason"] == "invalid_json"


def test_kafka_outage_returns_503_so_provider_retries():
    client = TestClient(create_app(FakeProducer(fail=True), SECRET))
    assert post(client, event()).status_code == 503


def test_metrics_endpoint(client_and_producer):
    client, _ = client_and_producer
    post(client, event())
    assert "webhook_events_total" in client.get("/metrics/").text


def test_event_dated_in_the_future_goes_to_dlq(client_and_producer):
    from datetime import datetime, timedelta, timezone
    client, producer = client_and_producer
    future = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    r = post(client, event(occurred_at=future))
    assert r.status_code == 400
    assert [(t, v["reason"]) for t, _, v in producer.sent] == [("payments.dlq", "occurred_in_future")]


def test_small_clock_skew_is_tolerated(client_and_producer):
    from datetime import datetime, timedelta, timezone
    client, producer = client_and_producer
    slightly_ahead = (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat()
    assert post(client, event(occurred_at=slightly_ahead)).status_code == 202
    assert producer.sent[0][0] == "payments.raw"


# ---------- heartbeats ----------

class FakeKafka:
    def __init__(self, partitions=3, fail=False):
        self.partitions = partitions
        self.fail = fail
        self.sent = []

    def partition_count(self, topic):
        if self.fail:
            raise RuntimeError("broker down")
        return self.partitions

    def send_to_partitions(self, topic, records):
        self.sent.extend((topic, p, v) for p, v in records)


def test_heartbeat_goes_to_every_partition_with_the_partition_count():
    from datetime import datetime, timezone

    at = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    kafka = FakeKafka(partitions=3)
    assert HeartbeatEmitter(kafka, "payments.raw", 10, clock=lambda: at).beat() == 3
    assert [(t, p) for t, p, _ in kafka.sent] == [("payments.raw", p) for p in range(3)]
    assert all(v == {"type": "heartbeat", "emitted_at": "2026-01-01T12:00:00+00:00",
                     "partition": p, "partitions": 3} for _, p, v in kafka.sent)


def test_heartbeat_failure_is_logged_not_raised():
    assert HeartbeatEmitter(FakeKafka(fail=True), "payments.raw", 10).beat_safely() is False
    assert HeartbeatEmitter(FakeKafka(), "payments.raw", 10).beat_safely() is True


def test_heartbeat_records_match_what_the_stream_parses(spark):
    """The webhook's message shape and the stream's parser must agree."""
    from datetime import datetime, timezone

    from reconstream.parsing import parse_heartbeats, parse_payment_events

    at = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    rows = [("payments.raw", p, json.dumps(v).encode(), at.replace(tzinfo=None))
            for p, v in heartbeat_records(2, at)]
    raw = spark.createDataFrame(rows, "topic string, partition int, value binary, timestamp timestamp")
    got = {(r.partition, r.emitted_at, r.partitions) for r in parse_heartbeats(raw).collect()}
    assert got == {(0, at.replace(tzinfo=None), 2), (1, at.replace(tzinfo=None), 2)}
    assert parse_payment_events(raw).count() == 0, "a heartbeat is never a payment"
