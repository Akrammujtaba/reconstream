import json
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "webhook"))
from app import create_app, sign  # noqa: E402

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
