"""Data quality checks on silver tables, run with Great Expectations before dbt publishes marts."""

from __future__ import annotations

import os
import uuid

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import great_expectations as gx  # noqa: E402
import great_expectations.expectations as gxe  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

CURRENCIES = ["USD", "EUR", "GBP", "INR"]


def payment_expectations() -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="booking_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="occurred_at"),
        gxe.ExpectColumnValuesToBeInSet(
            column="event_type",
            value_set=["payment.captured", "payment.failed", "refund.succeeded"],
        ),
        gxe.ExpectColumnValuesToBeBetween(column="amount_cents", min_value=0, max_value=10_000_000),
        gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=CURRENCIES),
    ]


def booking_expectations() -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="booking_id"),
        gxe.ExpectColumnValuesToBeInSet(
            column="status", value_set=["PENDING", "CONFIRMED", "CANCELLED"]
        ),
        gxe.ExpectColumnValuesToBeBetween(column="amount_cents", min_value=1, max_value=10_000_000),
        gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=CURRENCIES),
        # A confirmed booking must say when it was confirmed.
        gxe.ExpectColumnValuesToNotBeNull(
            column="confirmed_at", row_condition='status=="CONFIRMED"',
            condition_parser="spark",
        ),
    ]


def validate(df: DataFrame, name: str, expectations: list, unique_key: str | None = None) -> dict:
    """Run expectations against a Spark DataFrame. Returns a compact summary.

    Uniqueness is checked with Spark directly: in GX 1.2 the column-unique
    expectation is dropped as a "duplicate" of a not-null check on the same
    column, and the compound-unique one misreports on Spark.
    """
    # GX caches metrics by batch id, so every run gets unique names.
    run = f"{name}_{uuid.uuid4().hex[:8]}"
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_spark(name=f"{run}_source")
    asset = source.add_dataframe_asset(name=run)
    batch_def = asset.add_batch_definition_whole_dataframe(f"{run}_batch")
    suite = context.suites.add(gx.ExpectationSuite(name=f"{run}_suite"))
    for exp in expectations:
        suite.add_expectation(exp)
    result = batch_def.get_batch(batch_parameters={"dataframe": df}).validate(suite)

    if len(suite.expectations) != len(expectations):
        raise ValueError(f"{name}: GX dropped expectations it considered duplicates")

    failures = [
        {
            "expectation": r.expectation_config.type,
            "column": r.expectation_config.kwargs.get("column"),
            "unexpected_count": (r.result or {}).get("unexpected_count"),
        }
        for r in result.results
        if not r.success
    ]
    checks = len(result.results)
    if unique_key:
        checks += 1
        dupes = df.groupBy(unique_key).count().where(F.col("count") > 1).count()
        if dupes:
            failures.append({"expectation": "unique_key", "column": unique_key,
                             "unexpected_count": dupes})
    return {"table": name, "success": not failures, "checks": checks, "failures": failures}
