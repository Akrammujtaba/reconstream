"""Payment webhook receiver.

Verifies the provider's HMAC signature, validates the payload against a
contract, and publishes to Kafka keyed by booking_id (so every event for one
booking lands in the same partition, in order). Payloads that fail the
contract go to a dead-letter topic with the reason, instead of being dropped.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Literal, Protocol

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, make_asgi_app
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

log = logging.getLogger("webhook")

EVENTS = Counter("webhook_events_total", "Webhook deliveries by outcome", ["outcome"])
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
    return create_app(
        KafkaProducer(os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")),
        os.environ.get("WEBHOOK_SECRET", "dev-secret"),
        os.environ.get("PAYMENTS_TOPIC", "payments.raw"),
        os.environ.get("DLQ_TOPIC", "payments.dlq"),
        timedelta(seconds=int(os.environ.get("MAX_CLOCK_SKEW_SECONDS", "300"))),
    )

