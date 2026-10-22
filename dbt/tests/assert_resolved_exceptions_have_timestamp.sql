-- An exception is RESOLVED if and only if it has a resolved_at time.
select exception_id, status, resolved_at
from {{ ref('fct_recon_exceptions') }}
where (status = 'RESOLVED' and resolved_at is null)
   or (status = 'OPEN' and resolved_at is not null)
