"""Spark schemas for the two inbound streams.

Bookings arrive as Debezium change events (JSON converter, schemas disabled).
Payments arrive as validated webhook events produced by the ingestion service.
"""

from pyspark.sql.types import LongType, StringType, StructField, StructType

# Row image of public.bookings. Debezium renders TIMESTAMPTZ as ISO-8601 strings.
BOOKING_ROW = StructType(
    [
        StructField("booking_id", StringType()),
        StructField("guest_id", StringType()),
        StructField("listing_id", StringType()),
        StructField("status", StringType()),
        StructField("amount_cents", LongType()),
        StructField("currency", StringType()),
        StructField("created_at", StringType()),
        StructField("confirmed_at", StringType()),
        StructField("cancelled_at", StringType()),
        StructField("updated_at", StringType()),
    ]
)

DEBEZIUM_BOOKING_ENVELOPE = StructType(
    [
        StructField("before", BOOKING_ROW),
        StructField("after", BOOKING_ROW),
        StructField(
            "source",
            StructType([StructField("lsn", LongType()), StructField("ts_ms", LongType())]),
        ),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ]
)

PAYMENT_EVENT = StructType(
    [
        StructField("event_id", StringType()),
        StructField("type", StringType()),
        StructField("payment_id", StringType()),
        StructField("booking_id", StringType()),
        StructField("amount_cents", LongType()),
        StructField("currency", StringType()),
        StructField("occurred_at", StringType()),
        StructField("received_at", StringType()),
    ]
)
