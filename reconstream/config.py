"""Runtime settings, read from environment variables so every job and container shares one source."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    bookings_topic: str
    payments_topic: str
    alerts_topic: str
    catalog: str
    checkpoint_dir: str
    sla_minutes: int
    allowed_lateness_seconds: int
    horizon_minutes: int
    trigger_seconds: int
    max_offsets_per_trigger: int
    compact_every_batches: int
    pushgateway: str | None
    slack_webhook_url: str | None

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP", "kafka:9092"),
            bookings_topic=_env("BOOKINGS_TOPIC", "pg.public.bookings"),
            payments_topic=_env("PAYMENTS_TOPIC", "payments.raw"),
            alerts_topic=_env("ALERTS_TOPIC", "recon.alerts"),
            catalog=_env("ICEBERG_CATALOG", "lake"),
            checkpoint_dir=_env("CHECKPOINT_DIR", "/checkpoints/reconstream"),
            sla_minutes=int(_env("RECON_SLA_MINUTES", "2")),
            allowed_lateness_seconds=int(_env("RECON_ALLOWED_LATENESS_SECONDS", "30")),
            horizon_minutes=int(_env("RECON_HORIZON_MINUTES", "360")),
            trigger_seconds=int(_env("STREAM_TRIGGER_SECONDS", "15")),
            max_offsets_per_trigger=int(_env("STREAM_MAX_OFFSETS_PER_TRIGGER", "20000")),
            # Compact silver/gold every N non-empty batches (0 disables; nightly job still runs).
            compact_every_batches=int(_env("STREAM_COMPACT_EVERY_BATCHES", "40")),
            pushgateway=os.environ.get("PUSHGATEWAY_URL") or None,
            slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL") or None,
        )


# Fully qualified table names, relative to the Iceberg catalog.
BRONZE_EVENTS = "bronze.kafka_events"
SILVER_BOOKINGS = "silver.bookings"
SILVER_PAYMENTS = "silver.payments"
GOLD_EXCEPTIONS = "gold.recon_exceptions"


def fq(settings: Settings, table: str) -> str:
    return f"{settings.catalog}.{table}"
