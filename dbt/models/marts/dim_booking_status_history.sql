-- SCD Type 2 history of each booking's status and amount, built from the dbt snapshot.
select
    booking_id,
    status,
    amount_cents,
    currency,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('bookings_snapshot') }}
