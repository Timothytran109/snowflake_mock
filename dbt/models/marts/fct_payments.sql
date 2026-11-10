with trusted_subscriptions as (
    select subscription_id, customer_id
    from {{ ref('fct_subscriptions') }}
)

select
    payments.payment_id,
    payments.customer_id,
    payments.subscription_id,
    payments.amount,
    payments.currency,
    payments.status,
    payments.payment_at,
    payments.loaded_at
from {{ ref('int_stripe__payments') }} as payments
left join trusted_subscriptions
    on payments.subscription_id = trusted_subscriptions.subscription_id
    and payments.customer_id = trusted_subscriptions.customer_id
where not payments.is_orphan
    and (payments.subscription_id is null or trusted_subscriptions.subscription_id is not null)
