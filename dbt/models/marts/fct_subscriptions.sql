select
    subscription_id,
    customer_id,
    plan,
    status,
    start_date,
    end_date,
    loaded_at
from {{ ref('int_stripe__subscriptions') }}
where not is_orphan
