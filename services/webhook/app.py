"""Payment webhook receiver.

Verifies the provider's HMAC signature, validates the payload against a
contract, and publishes to Kafka keyed by booking_id (so every event for one
booking lands in the same partition, in order). Payloads that fail the
contract go to a dead-letter topic with the reason, instead of being dropped.

It also writes a heartbeat into every partition of the payments topic every few
seconds, so the stream's event-time watermark keeps moving when no payments arrive.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Literal, Protocol

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, make_asgi_app
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

log = logging.getLogger("webhook")

EVENTS = Counter("webhook_events_total", "Webhook deliveries by outcome", ["outcome"])
HEARTBEATS = Counter("webhook_heartbeats_total", "Heartbeat rounds by outcome", ["outcome"])
LATENCY = Histogram("webhook_request_seconds", "Time to validate and publish a delivery")


class PaymentEvent(BaseModel):
    """The contract every payment event must meet before it reaches Kafka."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1, max_length=64)
    type: Literal["payment.captured", "payment.failed", "refund.succeeded"]
    payment_id: str = Field(min_length=1, max_length=64)
    booking_id: str = Field(min_length=1, max_length=64)
    amount_cents: int = Field(ge=0, le=10_000_000)
    currency: Literal["USD", "EUR", "GBP", "INR"]
    occurred_at: AwareDatetime


class Producer(Protocol):
    def send(self, topic: str, key: str, value: dict) -> None: ...


class KafkaProducer:
    """Idempotent producer; waits for broker acknowledgement before the webhook returns 202."""

    def __init__(self, bootstrap: str):
        from confluent_kafka import Producer as _P

        self._p = _P({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
            "compression.type": "zstd",
        })

    def send(self, topic: str, key: str, value: dict) -> None:
        errors: list = []
        self._p.produce(
            topic, key=key.encode(), value=json.dumps(value).encode(),
            on_delivery=lambda err, _msg: errors.append(err) if err else None,
        )
        if self._p.flush(5) > 0 or errors:
            raise RuntimeError(f"kafka delivery failed: {errors or 'timeout'}")

    def partition_count(self, topic: str) -> int:
        meta = self._p.list_topics(topic, timeout=5).topics.get(topic)
        if meta is None or meta.error is not None or not meta.partitions:
            raise RuntimeError(f"no partitions for {topic}: {meta and meta.error}")
        return len(meta.partitions)

    def send_to_partitions(self, topic: str, records: list[tuple[int, dict]]) -> None:
        """Write each record to an explicit partition (no key), then wait for acks."""
        errors: list = []
        for partition, value in records:
            self._p.produce(
                topic, value=json.dumps(value).encode(), partition=partition,
                on_delivery=lambda err, _msg: errors.append(err) if err else None,
            )
        if self._p.flush(5) > 0 or errors:
            raise RuntimeError(f"kafka delivery failed: {errors or 'timeout'}")


def heartbeat_records(partitions: int, emitted_at: datetime) -> list[tuple[int, dict]]:
    """One heartbeat per partition. `partitions` travels with it so the stream knows
    how many partitions must report before it can trust the oldest heartbeat."""
    stamp = emitted_at.isoformat()
    return [
        (p, {"type": "heartbeat", "emitted_at": stamp, "partition": p, "partitions": partitions})
        for p in range(partitions)
    ]


class HeartbeatEmitter:
    """Every `interval_s`, write a heartbeat stamped with this server's clock into
    every partition of the payments topic.

    Why here: the webhook is the only writer to that topic, and Kafka keeps order
    per partition. A heartbeat stamped T is queued after every event this process
    had already published, so once the stream reads it, nothing older is still
    waiting in that partition. A request stamped just before T can still land
    after the heartbeat (requests run concurrently); the stream's allowed lateness
    (30s) covers that gap, as it already does for provider delays.
    """

    def __init__(self, producer, topic: str, interval_s: float,
                 clock=lambda: datetime.now(timezone.utc)):
        self.producer = producer
        self.topic = topic
        self.interval_s = interval_s
        self.clock = clock

    def beat(self) -> int:
        """Send one round. Returns how many partitions got a heartbeat."""
        partitions = self.producer.partition_count(self.topic)
        self.producer.send_to_partitions(self.topic, heartbeat_records(partitions, self.clock()))
        return partitions

    def beat_safely(self) -> bool:
        # A missed heartbeat only delays timeouts until the next one; it must never
        # take the webhook down with it.
        try:
            self.beat()
        except Exception:
            log.warning("heartbeat failed; retrying in %ss", self.interval_s, exc_info=True)
            HEARTBEATS.labels("error").inc()
            return False
        HEARTBEATS.labels("sent").inc()
        return True

    def run(self, stop: threading.Event) -> None:
        while not stop.wait(self.interval_s):
            self.beat_safely()


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def create_app(producer: Producer, secret: str, payments_topic: str = "payments.raw",
               dlq_topic: str = "payments.dlq",
               max_clock_skew: timedelta = timedelta(minutes=5)) -> FastAPI:
    """max_clock_skew: how far in the future occurred_at may be before the event is
    rejected. A future timestamp would push the event-time watermark ahead of real
    time and make every open booking look overdue, so it must never reach Kafka."""
    app = FastAPI(title="ReconStream payment webhooks")
    app.mount("/metrics", make_asgi_app())

    def reject(raw: bytes, reason: str, detail) -> JSONResponse:
        EVENTS.labels("rejected").inc()
        try:
            producer.send(dlq_topic, "dlq", {
                "reason": reason,
                "detail": detail,
                "payload": raw.decode("utf-8", errors="replace"),
                "received_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception:  # the DLQ is best effort; the 400 still tells the sender
            log.exception("could not write to DLQ")
        return JSONResponse({"status": "rejected", "reason": reason}, status_code=400)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/webhooks/payments")
    async def receive(request: Request):
        with LATENCY.time():
            raw = await request.body()
            expected = sign(secret, raw)
            if not hmac.compare_digest(request.headers.get("X-Signature", ""), expected):
                EVENTS.labels("unauthorized").inc()
                return JSONResponse({"status": "unauthorized"}, status_code=401)

            try:
                payload = json.loads(raw)
            except ValueError:
                return reject(raw, "invalid_json", None)
            try:
                event = PaymentEvent.model_validate(payload)
            except ValidationError as exc:
                detail = [{"field": ".".join(map(str, e["loc"])), "error": e["msg"]}
                          for e in exc.errors()]
                return reject(raw, "contract_violation", detail)

            now = datetime.now(timezone.utc)
            if event.occurred_at > now + max_clock_skew:
                return reject(raw, "occurred_in_future", {
                    "occurred_at": event.occurred_at.isoformat(),
                    "server_time": now.isoformat(),
                })

            record = event.model_dump(mode="json")
            record["received_at"] = now.isoformat()
            try:
                producer.send(payments_topic, event.booking_id, record)
            except Exception:
                # 5xx makes the provider retry later; the event_id keeps retries idempotent.
                log.exception("publish failed")
                EVENTS.labels("error").inc()
                return JSONResponse({"status": "retry"}, status_code=503)

            EVENTS.labels("accepted").inc()
            return JSONResponse({"status": "accepted", "event_id": event.event_id},
                                status_code=202)

    return app


def app_from_env() -> FastAPI:
    """Factory used by uvicorn: `uvicorn --factory app:app_from_env`."""
    logging.basicConfig(level=logging.INFO)
    producer = KafkaProducer(os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    payments_topic = os.environ.get("PAYMENTS_TOPIC", "payments.raw")
    # 10s is shorter than the stream's 15s trigger, so a quiet batch still sees one.
    interval = float(os.environ.get("HEARTBEAT_SECONDS", "10"))
    if interval > 0:
        emitter = HeartbeatEmitter(producer, payments_topic, interval)
        threading.Thread(target=emitter.run, args=(threading.Event(),), daemon=True,
                         name="heartbeat").start()
    return create_app(
        producer,
        os.environ.get("WEBHOOK_SECRET", "dev-secret"),
        payments_topic,
        os.environ.get("DLQ_TOPIC", "payments.dlq"),
        timedelta(seconds=int(os.environ.get("MAX_CLOCK_SKEW_SECONDS", "300"))),
    )

