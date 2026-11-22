# Finance queries

Five questions finance or payments-ops would ask the lakehouse, with the SQL to answer them. Run them in the Trino CLI:

```bash
docker compose exec trino trino --catalog lake
```

Most read the dbt marts (`marts.fct_recon_exceptions`), so run the nightly DAG (or `dbt build`) first. The marts already carry severity, owner team, USD exposure and how long each exception was open, so the queries stay short.

**Portability.** Every query uses only SQL that Trino and DuckDB both accept (`date_diff`, `round`, `cast(... as double)`, no `/` on integers, no `approx_percentile`). That is on purpose: `tests/test_finance_queries.py` pulls each `sql` block out of this file and runs it, unchanged, against the dbt CI fixtures on DuckDB, and checks the answers. So the docs can't rot when a column is renamed. The test proves the SQL and the logic, not Trino itself; on the live stack they read the real Iceberg tables.

**One caveat on "how long open".** `seconds_open` for an open exception is measured up to `last_evaluated_at`, the last time a reconciliation run looked at it, not up to now. If the stream is down, open exceptions stop aging. That is the right number for "how long has this been known", and it keeps the mart deterministic.

## 1. How much money is at risk right now, by currency?

Open exceptions only. Shows the exposure in the original currency and converted to USD with the `fx_rates` seed, so finance can see both what to chase and the total.

```sql
select
    risk_currency,
    count(*) as open_exceptions,
    round(sum(cast(amount_at_risk_cents as double)) / 100, 2) as at_risk_local,
    round(sum(amount_at_risk_usd), 2) as at_risk_usd
from marts.fct_recon_exceptions
where status = 'OPEN'
group by risk_currency
order by at_risk_usd desc, risk_currency
```

## 2. Which exception types are slowest to resolve?

Resolved exceptions only, from first detection to resolution. A type that takes hours to clear usually means a manual step (a refund someone has to approve), while one that clears in a minute is probably a late payment the stream resolved on its own.

```sql
select
    exception_type,
    count(*) as resolved,
    round(avg(cast(seconds_open as double)) / 60, 1) as avg_minutes_to_resolve,
    round(max(cast(seconds_open as double)) / 60, 1) as max_minutes_to_resolve
from marts.fct_recon_exceptions
where status = 'RESOLVED'
group by exception_type
order by avg_minutes_to_resolve desc, exception_type
```

## 3. What should ops work on first?

The open queue, most severe first, then oldest first within a severity. `owner_team` says who picks it up.

```sql
select
    exception_id,
    booking_id,
    exception_type,
    severity,
    owner_team,
    amount_at_risk_usd,
    first_detected_at,
    round(cast(seconds_open as double) / 60, 1) as minutes_open
from marts.fct_recon_exceptions
where status = 'OPEN'
order by
    case severity when 'critical' then 1 when 'high' then 2 when 'medium' then 3 else 4 end,
    first_detected_at,
    exception_id
```

## 4. How fast do we catch each kind of problem?

Seconds from the moment an exception became detectable (`due_at`, e.g. the end of the payment SLA) to when the pipeline flagged it. This is the same latency the evaluator measures against injected faults, but on real traffic, so it keeps working after the generator is switched off.

```sql
select
    exception_type,
    count(*) as detected,
    round(avg(cast(seconds_from_due_to_detection as double)), 1) as avg_seconds_to_detect,
    max(seconds_from_due_to_detection) as max_seconds_to_detect
from marts.fct_recon_exceptions
group by exception_type
order by avg_seconds_to_detect desc, exception_type
```

## 5. Which guests were charged twice and need a refund?

Open `DUPLICATE_CHARGE` exceptions joined to the booking for the guest. `payment_ids` lists every capture, so whoever issues the refund knows which charge to reverse. The refund is the extra amount (`amount_at_risk`), not the whole booking.

```sql
select
    e.booking_id,
    b.guest_id,
    e.payment_ids,
    e.risk_currency as currency,
    round(cast(e.amount_at_risk_cents as double) / 100, 2) as refund_local,
    e.amount_at_risk_usd as refund_usd,
    e.first_detected_at
from marts.fct_recon_exceptions as e
join silver.bookings as b
    on b.booking_id = e.booking_id
where e.exception_type = 'DUPLICATE_CHARGE'
  and e.status = 'OPEN'
order by e.amount_at_risk_usd desc, e.booking_id
```
