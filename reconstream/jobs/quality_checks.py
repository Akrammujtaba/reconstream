"""Validate silver tables for a date range; exit non-zero so Airflow stops before dbt runs.

    spark-submit reconstream/jobs/quality_checks.py --start 2026-01-01 --end 2026-01-02
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import functions as F  # noqa: E402

from reconstream.config import SILVER_BOOKINGS, SILVER_PAYMENTS, Settings  # noqa: E402
from reconstream.quality import booking_expectations, payment_expectations, validate  # noqa: E402
from reconstream.spark_session import build_spark  # noqa: E402


def _day(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time(), timezone.utc)


def main() -> None:
    os.environ["TZ"] = "UTC"
    time.tzset()
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    args = parser.parse_args()

    settings = Settings.from_env()
    spark = build_spark("reconstream-quality", settings.catalog)
    lo, hi = F.lit(_day(args.start)), F.lit(_day(args.end))

    payments = spark.table(f"{settings.catalog}.{SILVER_PAYMENTS}").where(
        (F.col("occurred_at") >= lo) & (F.col("occurred_at") < hi)
    )
    bookings = spark.table(f"{settings.catalog}.{SILVER_BOOKINGS}").where(
        (F.col("created_at") >= lo) & (F.col("created_at") < hi) & ~F.col("is_deleted")
    )
    results = [
        validate(payments, "silver_payments", payment_expectations(), unique_key="event_id"),
        validate(bookings, "silver_bookings", booking_expectations(), unique_key="booking_id"),
    ]
    print(json.dumps(results, indent=2))
    spark.stop()
    if not all(r["success"] for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
