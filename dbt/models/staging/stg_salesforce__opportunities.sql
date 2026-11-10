{{ config(tags = ['staging', 'salesforce']) }}

with cleaned as (

    select
        nullif(trim(opportunity_id::varchar), '') as opportunity_id,
        nullif(trim(account_id::varchar), '') as account_id,
        try_to_decimal(
            replace(replace(trim(amount::varchar), '$', ''), ',', ''), 18, 2
        ) as amount,
        nullif(
            regexp_replace(lower(trim(stage::varchar)), '[[:space:]-]+', '_'), ''
        ) as stage,
        {{ staging_timestamp('created_at') }} as created_at,
        {{ staging_timestamp('closed_at') }} as closed_at,
        _fivetran_synced as loaded_at,
        _line
    from {{ source('raw', 'mock_salesforce_opportunities') }}

)

select
    opportunity_id,
    account_id,
    amount,
    stage,
    created_at,
    closed_at,
    loaded_at,
    _line
from cleaned
qualify row_number() over (
    partition by opportunity_id
    order by loaded_at desc nulls last, created_at desc nulls last, _line desc
) = 1
