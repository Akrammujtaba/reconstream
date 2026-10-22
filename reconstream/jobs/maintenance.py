"""Iceberg table maintenance: compact small files, fold in delete files, expire old snapshots.

Streaming MERGEs every 10 seconds leave many small data and delete files. Left
alone, reads get slower every day. This runs nightly from Airflow.

    spark-submit reconstream/jobs/maintenance.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from reconstream.config import (  # noqa: E402
    BRONZE_EVENTS,
    GOLD_EXCEPTIONS,
    SILVER_BOOKINGS,
    SILVER_PAYMENTS,
    Settings,
)
from reconstream.spark_session import build_spark  # noqa: E402

log = logging.getLogger("reconstream.maintenance")

SNAPSHOT_RETENTION = timedelta(days=int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "3")))


def main() -> None:
    os.environ["TZ"] = "UTC"
    time.tzset()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    spark = build_spark("reconstream-maintenance", settings.catalog)
    cat = settings.catalog
    cutoff = (datetime.now(timezone.utc) - SNAPSHOT_RETENTION).strftime("%Y-%m-%d %H:%M:%S")

    for table in (SILVER_BOOKINGS, SILVER_PAYMENTS, GOLD_EXCEPTIONS, BRONZE_EVENTS):
        log.info("maintaining %s", table)
        # Rewrites small files and applies position deletes from merge-on-read writes.
        spark.sql(
            f"CALL {cat}.system.rewrite_data_files(table => '{table}', "
            "options => map('min-input-files', '5', 'delete-file-threshold', '1'))"
        ).show(truncate=False)
        spark.sql(
            f"CALL {cat}.system.rewrite_position_delete_files(table => '{table}')"
        ).show(truncate=False)
        spark.sql(
            f"CALL {cat}.system.expire_snapshots(table => '{table}', "
            f"older_than => TIMESTAMP '{cutoff}', retain_last => 20)"
        ).show(truncate=False)
        spark.sql(f"CALL {cat}.system.rewrite_manifests(table => '{table}')").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
