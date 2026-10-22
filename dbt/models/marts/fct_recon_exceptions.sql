-- One row per exception with business context: severity, owner, USD exposure,
-- and how long it stayed open.
with exceptions as (
    select * from {{ ref('stg_recon_exceptions') }}
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
    e.reopen_count
from exceptions as e
left join types as t
    on e.exception_type = t.exception_type
left join fx
    on fx.currency = coalesce(e.booking_currency, e.payment_currency)
