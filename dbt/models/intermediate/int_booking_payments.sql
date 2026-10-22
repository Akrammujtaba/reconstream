-- Per-booking payment totals. Captures are de-duplicated on payment_id, the same
-- way the Spark reconciliation does it, so the two never disagree.
with ranked as (
    select
        p.*,
        row_number() over (partition by payment_id order by occurred_at, event_id) as rn
    from {{ ref('stg_payments') }} as p
    where event_type in ('payment.captured', 'refund.succeeded')
),

payments as (
    select * from ranked where rn = 1
)

select
    booking_id,
    count(distinct case when event_type = 'payment.captured' then payment_id end) as n_captures,
    sum(case when event_type = 'payment.captured' then amount_cents else 0 end) as captured_cents,
    sum(case when event_type = 'refund.succeeded' then amount_cents else 0 end) as refunded_cents,
    min(case when event_type = 'payment.captured' then occurred_at end) as first_captured_at,
    max(case when event_type = 'payment.captured' then delivery_delay_seconds end) as max_capture_delay_seconds
from payments
group by booking_id
