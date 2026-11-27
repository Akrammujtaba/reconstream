-- Aging report for open exceptions: how much money has been at risk, and for
-- how long, by exception type. The reconciliation version of an AR aging
-- report: money stuck for a week needs a different conversation than money
-- stuck for ten minutes.
--
-- Why age is measured against one shared "as of" and not seconds_open:
-- seconds_open for an open exception runs up to that row's own
-- last_evaluated_at. The stream only re-evaluates bookings that got new events
-- or just hit their deadline, so an untouched open exception keeps the
-- last_evaluated_at of whenever it was last looked at (up to a day old, until
-- the nightly batch re-checks everything). Ages built from seconds_open would
-- each be measured to a different clock, and the oldest rows would look the
-- youngest. So every row is aged to the same point: the newest
-- last_evaluated_at in the whole table, i.e. the moment the data is known to
-- be current through.
--
-- Not current_timestamp: that would make the mart change on every rebuild with
-- no new data, and if the pipeline stalls it would age exceptions the pipeline
-- may already have resolved without our knowing. Same choice docs/queries.md
-- makes for "how long open".
--
-- Bucket edges are lower-inclusive: exactly 1 hour old is "1-24h".

with exceptions as (
    select * from {{ ref('fct_recon_exceptions') }}
),

as_of as (
    select max(last_evaluated_at) as as_of from exceptions
),

aged as (
    select
        e.exception_type,
        e.severity,
        e.owner_team,
        e.amount_at_risk_usd,
        e.first_detected_at,
        a.as_of,
        date_diff('second', e.first_detected_at, a.as_of) as age_seconds
    from exceptions as e
    cross join as_of as a
    where e.status = 'OPEN'
),

bucketed as (
    select
        *,
        case
            when age_seconds < 3600 then 1
            when age_seconds < 86400 then 2
            when age_seconds < 604800 then 3
            else 4
        end as age_bucket_order
    from aged
)

select
    exception_type,
    severity,
    owner_team,
    age_bucket_order,
    case age_bucket_order
        when 1 then '0-1h'
        when 2 then '1-24h'
        when 3 then '1-7d'
        else '7d+'
    end as age_bucket,
    count(*) as open_exceptions,
    round(sum(amount_at_risk_usd), 2) as at_risk_usd,
    min(first_detected_at) as oldest_detected_at,
    max(age_seconds) as max_age_seconds,
    max(as_of) as as_of
from bucketed
group by exception_type, severity, owner_team, age_bucket_order
