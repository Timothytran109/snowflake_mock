with customers as (
    select distinct customer_id
    from {{ ref('stg_stripe__customers') }}
),

checked as (
    select
        payments.payment_id,
        payments.customer_id,
        payments.email,
        payments.subscription_id,
        payments.amount,
        payments.currency,
        payments.status,
        payments.payment_at,
        payments.loaded_at,
        customers.customer_id is null as is_customer_orphan,
        payments.subscription_id is not null
            and subscriptions.subscription_id is null as is_subscription_missing,
        coalesce(subscriptions.is_orphan, false) as is_subscription_orphan,
        subscriptions.subscription_id is not null
            and not equal_null(payments.customer_id, subscriptions.customer_id)
            as is_subscription_customer_mismatch
    from {{ ref('stg_stripe__payments') }} as payments
    left join customers
        on payments.customer_id = customers.customer_id
    -- The upstream subscription primary-key tests enforce a many-to-one join.
    left join {{ ref('int_stripe__subscriptions') }} as subscriptions
        on payments.subscription_id = subscriptions.subscription_id
)

select
    payment_id,
    customer_id,
    email,
    subscription_id,
    amount,
    currency,
    status,
    payment_at,
    loaded_at,
    is_customer_orphan,
    is_subscription_missing,
    is_subscription_orphan,
    is_subscription_customer_mismatch,
    is_customer_orphan or is_subscription_missing or is_subscription_orphan
        or is_subscription_customer_mismatch as is_orphan
from checked
