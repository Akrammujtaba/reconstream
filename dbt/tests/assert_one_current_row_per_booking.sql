-- SCD2 integrity: every booking has exactly one current version.
select booking_id, count(*) as current_rows
from {{ ref('dim_booking_status_history') }}
where is_current
group by booking_id
having count(*) <> 1
