select
    booking_id,
    guest_id,
    listing_id,
    status,
    amount_cents,
    currency,
    created_at,
    confirmed_at,
    cancelled_at,
    updated_at
from {{ source('silver', 'bookings') }}
where not coalesce(is_deleted, false)
