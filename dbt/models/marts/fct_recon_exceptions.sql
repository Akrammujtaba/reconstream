-- One row per exception with business context: severity, owner, USD exposure,
-- and how long it stayed open.
--
-- Incremental, not a full rebuild. gold.recon_exceptions keeps every exception
-- it has ever seen (resolved ones are never deleted), so a nightly full rebuild
-- re-reads and rewrites the whole history to pick up one day of changes.
--
-- Which rows changed: the gold MERGE (reconstream/lakehouse.py) sets
-- last_evaluated_at on every insert, update, and resolve, so it is the one
-- column that moves whenever any other column here can move. seconds_open also
-- depends on it for open exceptions, so those get refreshed by the same filter.
--
-- Why a lookback and not just "> max(last_evaluated_at)": evaluated_at is taken
-- when a writer starts its batch, not when its MERGE commits. A stream batch can
-- stamp time T and commit after the nightly batch has already written a later
-- time. A strict "newer than the max" filter would then skip the stream's rows
-- forever. Re-reading the last `recon_exceptions_lookback_minutes` covers that
-- gap; re-processing a row twice is harmless because the key is exception_id.
--
-- delete+insert rather than merge: both dbt-trino and dbt-duckdb (the CI
-- target) support it, and it replaces the whole row, which is what we want.
--
-- Limits: rows whose last_evaluated_at falls outside the lookback are not
-- re-joined to the seeds, so after editing exception_types.csv or fx_rates.csv
-- run `dbt build --full-refresh -s fct_recon_exceptions` once.
{{
    config(
        materialized='incremental',
        unique_key='exception_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

with exceptions as (
    select * from {{ ref('stg_recon_exceptions') }}
    {% if is_incremental() %}
    where last_evaluated_at >= (
        select coalesce(max(last_evaluated_at), cast('1970-01-01' as timestamp))
        from {{ this }}
    ) - interval '{{ var("recon_exceptions_lookback_minutes", 60) | int }}' minute
    {% endif %}
),

types as (
    select * from {{ ref('exception_types') }}
),

fx as (
    select * from {{ ref('fx_rates') }}
)

select
    e.exception_id,
    e.booking_id,
    e.exception_type,
    t.severity,
    t.owner_team,
    e.status,
    e.expected_amount_cents,
    e.actual_amount_cents,
    e.amount_at_risk_cents,
    coalesce(e.booking_currency, e.payment_currency) as risk_currency,
    round(cast(e.amount_at_risk_cents as double) / 100 * coalesce(fx.usd_rate, 1.0), 2) as amount_at_risk_usd,
    e.payment_ids,
    e.due_at,
    e.first_detected_at,
    e.resolved_at,
    cast(e.first_detected_at as date) as detected_date,
    date_diff('second', e.due_at, e.first_detected_at) as seconds_from_due_to_detection,
    date_diff('second', e.first_detected_at, coalesce(e.resolved_at, e.last_evaluated_at)) as seconds_open,
    e.detected_by,
    e.reopen_count,
    -- Kept so the next incremental run can find where this one left off.
    e.last_evaluated_at
from exceptions as e
left join types as t
    on e.exception_type = t.exception_type
left join fx
    on fx.currency = coalesce(e.booking_currency, e.payment_currency)
