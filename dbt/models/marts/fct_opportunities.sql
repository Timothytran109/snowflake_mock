select
    opportunity_id,
    account_id,
    amount,
    stage,
    created_at,
    closed_at,
    loaded_at
from {{ ref('int_salesforce__opportunities') }}
where not is_orphan
