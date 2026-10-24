"""Entry point: Kafka -> Spark Structured Streaming -> Iceberg, reconciling every micro-batch.

    spark-submit reconstream/jobs/streaming_job.py
"""

from __future__ import annotations

import logging
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from reconstream.config import Settings  # noqa: E402
from reconstream.spark_session import build_spark  # noqa: E402
from reconstream.store import IcebergStore  # noqa: E402
from reconstream.stream_processor import StreamProcessor  # noqa: E402
from reconstream.tables import ensure_tables  # noqa: E402


def main() -> None:
    os.environ["TZ"] = "UTC"
    time.tzset()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()

    spark = build_spark("reconstream-stream", settings.catalog)
    ensure_tables(spark, settings.catalog)

    store = IcebergStore(spark, settings.catalog, settings.kafka_bootstrap, settings.alerts_topic)
    processor = StreamProcessor(store, settings)

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", f"{settings.bookings_topic},{settings.payments_topic}")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        source.writeStream.queryName("reconstream")
        .foreachBatch(processor.process)
        .option("checkpointLocation", settings.checkpoint_dir)
        .trigger(processingTime=f"{settings.trigger_seconds} seconds")
        .start()
    )
    logging.getLogger("reconstream").info(
        "streaming started: topics=%s,%s sla=%sm sla_by_currency=%s lateness=%ss",
        settings.bookings_topic, settings.payments_topic,
        settings.sla_minutes, dict(settings.sla_minutes_by_currency),
        settings.allowed_lateness_seconds,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
