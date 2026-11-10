{{ config(tags = ['staging', 'stripe']) }}

with cleaned as (

    select
        nullif(trim(subscription_id::varchar), '') as subscription_id,
        nullif(trim(customer_id::varchar), '') as customer_id,
        nullif(lower(trim(plan::varchar)), '') as plan,
        nullif(lower(trim(status::varchar)), '') as status,
        {{ staging_timestamp('start_date') }}::date as start_date,
        {{ staging_timestamp('end_date') }}::date as end_date,
        _fivetran_synced as loaded_at,
        _line
    from {{ source('raw', 'mock_stripe_subscriptions') }}

)

select
    subscription_id,
    customer_id,
    plan,
    status,
    start_date,
    end_date,
    loaded_at,
    _line
from cleaned
qualify row_number() over (
    partition by subscription_id
    order by loaded_at desc nulls last, start_date desc nulls last, _line desc
) = 1
