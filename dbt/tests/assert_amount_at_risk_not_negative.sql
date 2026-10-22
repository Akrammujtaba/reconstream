select exception_id, amount_at_risk_cents
from {{ ref('fct_recon_exceptions') }}
where amount_at_risk_cents < 0
