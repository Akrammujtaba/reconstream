select
    event_id,
    event_type,
    payment_id,
    booking_id,
    amount_cents,
    currency,
    occurred_at,
    received_at,
    date_diff('second', occurred_at, received_at) as delivery_delay_seconds
from {{ source('silver', 'payments') }}
