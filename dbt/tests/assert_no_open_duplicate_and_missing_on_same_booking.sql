-- A booking can't be both double-charged and never charged.
-- The stream raises MISSING_PAYMENT only when a booking has 0 captures and
-- DUPLICATE_CHARGE only when it has 2+, so both being OPEN at once means a
-- MISSING_PAYMENT was not resolved when the late captures landed. Ops would
-- then see two contradictory tickets for one booking, and exposure would be
-- counted twice in the open-risk numbers.
select
    booking_id,
    count(distinct exception_type) as open_conflicting_types
from {{ ref('fct_recon_exceptions') }}
where status = 'OPEN'
  and exception_type in ('DUPLICATE_CHARGE', 'MISSING_PAYMENT')
group by booking_id
having count(distinct exception_type) > 1
