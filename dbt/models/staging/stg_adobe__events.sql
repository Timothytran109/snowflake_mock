{{ config(tags = ['staging', 'adobe']) }}

with cleaned as (

    select
        nullif(trim(event_id::varchar), '') as event_id,
        nullif(lower(trim(email::varchar)), '') as email,
        nullif(
            regexp_replace(lower(trim(event_type::varchar)), '[[:space:]-]+', '_'), ''
        ) as event_type,
        nullif(lower(trim(device_type::varchar)), '') as device_type,
        nullif(trim(campaign_id::varchar), '') as campaign_id,
        {{ staging_timestamp('event_timestamp') }} as event_timestamp,
        _fivetran_synced as loaded_at,
        _line
    from {{ source('raw', 'mock_adobe_events') }}

)

select
    event_id,
    email,
    event_type,
    device_type,
    campaign_id,
    event_timestamp,
    loaded_at,
    _line
from cleaned
qualify row_number() over (
    partition by event_id
    order by loaded_at desc nulls last, event_timestamp desc nulls last, _line desc
) = 1
