{{ config(tags = ['staging', 'stripe']) }}

with cleaned as (

    select
        nullif(trim(customer_id::varchar), '') as customer_id,
        nullif(lower(trim(email::varchar)), '') as email,
        {{ staging_timestamp('created_at') }} as created_at,
        _fivetran_synced as loaded_at,
        _line
    from {{ source('raw', 'mock_stripe_customers') }}

)

select
    customer_id,
    email,
    created_at,
    loaded_at,
    _line
from cleaned
qualify row_number() over (
    partition by customer_id
    order by loaded_at desc nulls last, created_at desc nulls last, _line desc
) = 1
