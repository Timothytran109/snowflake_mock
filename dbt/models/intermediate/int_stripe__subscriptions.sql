with customers as (
    select distinct customer_id
    from {{ ref('stg_stripe__customers') }}
)

select
    subscriptions.subscription_id,
    subscriptions.customer_id,
    subscriptions.plan,
    subscriptions.status,
    subscriptions.start_date,
    subscriptions.end_date,
    subscriptions.loaded_at,
    subscriptions._line,
    customers.customer_id is null as is_orphan
from {{ ref('stg_stripe__subscriptions') }} as subscriptions
left join customers
    on subscriptions.customer_id = customers.customer_id
