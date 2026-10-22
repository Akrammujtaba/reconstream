-- Daily finance view: how much was booked, how much was paid cleanly, and what
-- went wrong. This is the table finance would look at instead of a month-end close.
with fx as (
    select * from {{ ref('fx_rates') }}
),

bookings as (
    select
        b.*,
        cast(b.created_at as date) as activity_date,
        coalesce(fx.usd_rate, 1.0) as usd_rate
    from {{ ref('stg_bookings') }} as b
    left join fx on fx.currency = b.currency
),

daily_bookings as (
    select
        activity_date,
        count(*) as bookings_created,
        sum(case when status = 'CONFIRMED' then 1 else 0 end) as bookings_confirmed,
        sum(case when status = 'CANCELLED' then 1 else 0 end) as bookings_cancelled,
        round(sum(case when status = 'CONFIRMED'
                       then cast(amount_cents as double) / 100 * usd_rate else 0 end), 2) as confirmed_gross_usd
    from bookings
    group by activity_date
),

daily_matching as (
    select
        b.activity_date,
        sum(case when p.n_captures = 1 and p.captured_cents = b.amount_cents then 1 else 0 end) as cleanly_paid
    from bookings as b
    left join {{ ref('int_booking_payments') }} as p
        on b.booking_id = p.booking_id
    where b.status = 'CONFIRMED'
    group by b.activity_date
),

daily_exceptions as (
    select
        detected_date,
        count(*) as exceptions_detected,
        sum(case when status = 'OPEN' then 1 else 0 end) as exceptions_open,
        sum(case when status = 'RESOLVED' then 1 else 0 end) as exceptions_resolved,
        {% for t in ['DUPLICATE_CHARGE', 'AMOUNT_MISMATCH', 'CURRENCY_MISMATCH', 'MISSING_PAYMENT', 'REFUND_MISMATCH', 'ORPHAN_PAYMENT'] %}
        sum(case when exception_type = '{{ t }}' then 1 else 0 end) as {{ t | lower }}_count,
        {% endfor %}
        round(sum(case when status = 'OPEN' then amount_at_risk_usd else 0 end), 2) as open_amount_at_risk_usd
    from {{ ref('fct_recon_exceptions') }}
    group by detected_date
)

select
    d.activity_date,
    d.bookings_created,
    d.bookings_confirmed,
    d.bookings_cancelled,
    d.confirmed_gross_usd,
    coalesce(m.cleanly_paid, 0) as bookings_cleanly_paid,
    round(cast(coalesce(m.cleanly_paid, 0) as double) / nullif(d.bookings_confirmed, 0), 4) as clean_payment_rate,
    coalesce(e.exceptions_detected, 0) as exceptions_detected,
    coalesce(e.exceptions_open, 0) as exceptions_open,
    coalesce(e.exceptions_resolved, 0) as exceptions_resolved,
    {% for t in ['DUPLICATE_CHARGE', 'AMOUNT_MISMATCH', 'CURRENCY_MISMATCH', 'MISSING_PAYMENT', 'REFUND_MISMATCH', 'ORPHAN_PAYMENT'] %}
    coalesce(e.{{ t | lower }}_count, 0) as {{ t | lower }}_count,
    {% endfor %}
    coalesce(e.open_amount_at_risk_usd, 0) as open_amount_at_risk_usd
from daily_bookings as d
left join daily_matching as m
    on d.activity_date = m.activity_date
left join daily_exceptions as e
    on d.activity_date = e.detected_date
