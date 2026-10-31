"""Runtime settings, read from environment variables so every job and container shares one source."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def parse_sla_overrides(raw: str) -> dict[str, int]:
    """Parse per-currency SLA overrides such as "INR=5,EUR=3" into {"INR": 5, "EUR": 3}.

    Payment rails settle at different speeds (UPI/INR flows can lag card captures),
    so one global SLA either flags slow-but-healthy markets or waits too long on fast
    ones. A malformed value raises at startup instead of silently falling back to the
    default, because a typo here would quietly change what counts as an exception.
    """
    overrides: dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        currency, sep, minutes = part.partition("=")
        currency = currency.strip().upper()
        if not sep or len(currency) != 3 or not currency.isalpha():
            raise ValueError(f"bad SLA override {part!r}: expected CCY=MINUTES, e.g. INR=5")
        try:
            value = int(minutes)
        except ValueError:
            raise ValueError(f"bad SLA override {part!r}: minutes must be an integer") from None
        if value <= 0:
            raise ValueError(f"bad SLA override {part!r}: minutes must be positive")
        overrides[currency] = value
    return overrides


def parse_refund_tolerance(raw: str) -> int:
    """Parse RECON_REFUND_TOLERANCE_CENTS: a non-negative whole number of minor units.

    Like the SLA overrides, a bad value fails at startup. A negative tolerance would
    make every exact refund look like a mismatch, which is worse than no tolerance.
    """
    try:
        value = int(raw.strip() or "0")
    except ValueError:
        raise ValueError(f"bad refund tolerance {raw!r}: expected whole cents, e.g. 1") from None
    if value < 0:
        raise ValueError(f"bad refund tolerance {raw!r}: must be >= 0")
    return value


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
    # Currency -> SLA minutes. Currencies not listed use `sla_minutes`.
    sla_minutes_by_currency: Mapping[str, int] = field(default_factory=dict)
    # Compact as soon as any table has this many open delete files (0 disables).
    compact_delete_file_threshold: int = 0
    # Refund vs. capture differences up to this many cents are not a REFUND_MISMATCH.
    refund_tolerance_cents: int = 0

    def sla_for(self, currency: str | None) -> int:
        """SLA minutes for a currency, falling back to the global default."""
        return self.sla_minutes_by_currency.get((currency or "").upper(), self.sla_minutes)

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
            sla_minutes_by_currency=parse_sla_overrides(_env("RECON_SLA_MINUTES_BY_CURRENCY", "")),
            # Every merge-on-read MERGE that updates rows adds a delete file, and every read
            # has to open all of them, so delete files are what actually slows the stream.
            # 20 is a starting point, not a tuned value; the batch count above stays as a
            # fallback for tables that only grow data files (bronze, insert-only payments).
            compact_delete_file_threshold=int(_env("COMPACT_DELETE_FILE_THRESHOLD", "20")),
            # Default 0 keeps the exact-match rule; set to 1 to absorb rounding on partial refunds.
            refund_tolerance_cents=parse_refund_tolerance(_env("RECON_REFUND_TOLERANCE_CENTS", "0")),
        )


# Fully qualified table names, relative to the Iceberg catalog.
BRONZE_EVENTS = "bronze.kafka_events"
SILVER_BOOKINGS = "silver.bookings"
SILVER_PAYMENTS = "silver.payments"
GOLD_EXCEPTIONS = "gold.recon_exceptions"


def fq(settings: Settings, table: str) -> str:
    return f"{settings.catalog}.{table}"
