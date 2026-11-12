{{ config(
    materialized = 'view',
    tags = ['staging', 'stripe']
) }}

with source_data as (

    select
        nullif(trim(payment_id::varchar), '') as payment_id,
        nullif(trim(customer_id::varchar), '') as customer_id,
        nullif(lower(trim(email::varchar)), '') as email,
        nullif(trim(subscription_id::varchar), '') as subscription_id,
        nullif(trim(amount::varchar), '') as raw_amount,
        nullif(lower(trim(currency::varchar)), '') as currency,
        nullif(lower(trim(status::varchar)), '') as status,
        nullif(trim(payment_timestamp::varchar), '') as raw_payment_timestamp,
        _fivetran_synced,
        _line

    from {{ source('raw', 'mock_stripe_payments') }}

),

typed_data as (

    select
        payment_id,
        customer_id,
        email,
        subscription_id,

        try_to_decimal(
            regexp_replace(raw_amount, '[^0-9.-]', ''),
            18,
            2
        ) as amount,

        upper(currency) as currency,
        status,

        coalesce(
            try_to_timestamp_tz(raw_payment_timestamp)::timestamp_ntz,
            try_to_timestamp_ntz(
                raw_payment_timestamp,
                'MM/DD/YYYY HH24:MI'
            ),
            try_to_timestamp_ntz(
                raw_payment_timestamp,
                'YYYY/MM/DD HH24:MI:SS'
            ),
            try_to_timestamp_ntz(
                raw_payment_timestamp,
                'YYYY-MM-DD HH24:MI:SS'
            ),
            try_to_timestamp_ntz(
                raw_payment_timestamp,
                'YYYY-MM-DD'
            )
        ) as payment_at,

        _fivetran_synced,
        _line

    from source_data

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
    _fivetran_synced as loaded_at

from typed_data

qualify row_number() over (
    partition by payment_id
    order by
        _fivetran_synced desc,
        payment_at desc nulls last,
        _line desc nulls last,
        -- Lines may repeat across files/sheets. Compare every remaining output
        -- value to resolve conflicts; normalized-identical rows are equivalent.
        collate(customer_id, 'utf8') desc nulls last,
        collate(email, 'utf8') desc nulls last,
        collate(subscription_id, 'utf8') desc nulls last,
        amount desc nulls last,
        collate(currency, 'utf8') desc nulls last,
        collate(status, 'utf8') desc nulls last
) = 1
