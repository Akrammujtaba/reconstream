{% snapshot bookings_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='booking_id',
        strategy='check',
        check_cols=['status', 'amount_cents', 'currency'],
    )
}}
select booking_id, status, amount_cents, currency, updated_at
from {{ source('silver', 'bookings') }}
where not coalesce(is_deleted, false)
{% endsnapshot %}
