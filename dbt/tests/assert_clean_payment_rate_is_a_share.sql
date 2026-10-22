select activity_date, clean_payment_rate
from {{ ref('fct_daily_reconciliation') }}
where clean_payment_rate < 0 or clean_payment_rate > 1
