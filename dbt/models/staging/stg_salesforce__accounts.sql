{{ config(tags = ['staging', 'salesforce']) }}

with cleaned as (

    select
        nullif(trim(account_id::varchar), '') as account_id,
        nullif(lower(trim(email::varchar)), '') as email,
        nullif(trim(company_name::varchar), '') as company_name,
        nullif(trim(industry::varchar), '') as industry,
        {{ staging_timestamp('created_at') }} as created_at,
        {{ staging_timestamp('updated_at') }} as updated_at,
        _fivetran_synced as loaded_at,
        _line
    from {{ source('raw', 'mock_salesforce_accounts') }}

)

select
    account_id,
    email,
    company_name,
    industry,
    created_at,
    updated_at,
    loaded_at,
    _line
from cleaned
-- Shared emails are valid: account_id is the source business key.
qualify row_number() over (
    partition by account_id
    order by
        loaded_at desc nulls last,
        updated_at desc nulls last,
        created_at desc nulls last,
        _line desc
) = 1
