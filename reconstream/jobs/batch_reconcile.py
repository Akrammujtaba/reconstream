"""Nightly / backfill reconciliation over a date range.

    spark-submit reconstream/jobs/batch_reconcile.py --start 2026-01-01 --end 2026-01-02
    spark-submit reconstream/jobs/batch_reconcile.py --full
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from reconstream.batch import run_batch  # noqa: E402
from reconstream.config import Settings  # noqa: E402
from reconstream.spark_session import build_spark  # noqa: E402
from reconstream.store import IcebergStore  # noqa: E402
from reconstream.tables import ensure_tables  # noqa: E402


def main() -> None:
    os.environ["TZ"] = "UTC"
    time.tzset()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--full", action="store_true", help="reconcile everything")
    args = parser.parse_args()
    if not args.full and not (args.start and args.end):
        parser.error("pass --start and --end, or --full")

    settings = Settings.from_env()
    spark = build_spark("reconstream-batch", settings.catalog)
    ensure_tables(spark, settings.catalog)
    store = IcebergStore(spark, settings.catalog, settings.kafka_bootstrap, settings.alerts_topic)

    stats = run_batch(store, settings, None if args.full else args.start,
                      None if args.full else args.end)
    print(json.dumps(stats))
    spark.stop()


if __name__ == "__main__":
    main()
