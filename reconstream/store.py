"""Storage boundary for the stream processor.

`IcebergStore` is what runs in production. Tests use an in-memory store with
the same interface, so the processor's logic is tested without Iceberg.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from reconstream import lakehouse
from reconstream.config import (
    BRONZE_EVENTS,
    GOLD_EXCEPTIONS,
    SILVER_BOOKINGS,
    SILVER_PAYMENTS,
)
from reconstream.reconcile import EXCEPTION_COLUMNS

ALERT_COLUMNS = EXCEPTION_COLUMNS + ["evaluated_at", "detected_by"]


class IcebergStore:
    def __init__(self, spark: SparkSession, catalog: str, kafka_bootstrap: str | None = None,
                 alerts_topic: str | None = None):
        self.spark = spark
        self.catalog = catalog
        self.kafka_bootstrap = kafka_bootstrap
        self.alerts_topic = alerts_topic

    def _t(self, name: str) -> str:
        return f"{self.catalog}.{name}"

    def bookings(self) -> DataFrame:
        return self.spark.table(self._t(SILVER_BOOKINGS))

    def payments(self) -> DataFrame:
        return self.spark.table(self._t(SILVER_PAYMENTS))

    def exceptions(self) -> DataFrame:
        return self.spark.table(self._t(GOLD_EXCEPTIONS))

    def append_bronze(self, raw: DataFrame, batch_id: int) -> None:
        lakehouse.append_bronze(raw, self._t(BRONZE_EVENTS), batch_id)

    def merge_bookings(self, latest: DataFrame) -> None:
        lakehouse.merge_bookings(latest, self._t(SILVER_BOOKINGS))

    def merge_payments(self, events: DataFrame) -> None:
        lakehouse.merge_payments(events, self._t(SILVER_PAYMENTS))

    def merge_exceptions(self, changes: DataFrame) -> None:
        lakehouse.merge_exceptions(changes, self._t(GOLD_EXCEPTIONS))

    def maintain(self, older_than: datetime) -> None:
        """Compact small data files, fold in delete files, and drop old snapshots.

        Streaming MERGEs commit every few seconds; without this, file counts and
        table metadata grow every batch and reads keep getting slower.
        """
        cutoff = older_than.strftime("%Y-%m-%d %H:%M:%S")
        for table in (SILVER_BOOKINGS, SILVER_PAYMENTS, GOLD_EXCEPTIONS, BRONZE_EVENTS):
            self.spark.sql(
                f"CALL {self.catalog}.system.rewrite_data_files(table => '{table}', "
                "options => map('min-input-files', '2', 'delete-file-threshold', '1'))"
            ).collect()
            self.spark.sql(
                f"CALL {self.catalog}.system.rewrite_position_delete_files(table => '{table}')"
            ).collect()
            self.spark.sql(
                f"CALL {self.catalog}.system.expire_snapshots(table => '{table}', "
                f"older_than => TIMESTAMP '{cutoff}', retain_last => 10)"
            ).collect()

    def publish_alerts(self, alerts: DataFrame) -> None:
        if not (self.kafka_bootstrap and self.alerts_topic):
            return
        (
            alerts.select(
                F.col("booking_id").alias("key"),
                F.to_json(F.struct(*ALERT_COLUMNS)).alias("value"),
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap)
            .option("topic", self.alerts_topic)
            .save()
        )
