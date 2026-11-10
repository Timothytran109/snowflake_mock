{{ config(severity = 'error') }}

select payments.payment_id
from {{ ref('fct_payments') }} as payments
inner join {{ ref('fct_subscriptions') }} as subscriptions
    on payments.subscription_id = subscriptions.subscription_id
where not equal_null(payments.customer_id, subscriptions.customer_id)
