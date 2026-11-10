-- Exact source IDs define identity; email is not used for parent matching.
with accounts as (
    select distinct account_id
    from {{ ref('stg_salesforce__accounts') }}
)

select
    opportunities.opportunity_id,
    opportunities.account_id,
    opportunities.amount,
    opportunities.stage,
    opportunities.created_at,
    opportunities.closed_at,
    opportunities.loaded_at,
    opportunities._line,
    accounts.account_id is null as is_orphan
from {{ ref('stg_salesforce__opportunities') }} as opportunities
left join accounts
    on opportunities.account_id = accounts.account_id
