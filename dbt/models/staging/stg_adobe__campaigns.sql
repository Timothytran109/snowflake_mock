{{ config(tags = ['staging', 'adobe']) }}

with cleaned as (

    select
        nullif(trim(campaign_id::varchar), '') as campaign_id,
        nullif(trim(campaign_name::varchar), '') as campaign_name,
        nullif(
            regexp_replace(lower(trim(channel::varchar)), '[[:space:]-]+', '_'), ''
        ) as channel,
        {{ staging_timestamp('start_date') }}::date as start_date,
        {{ staging_timestamp('end_date') }}::date as end_date,
        _fivetran_synced as loaded_at,
        _line
    from {{ source('raw', 'mock_adobe_campaigns') }}

)

select
    campaign_id,
    campaign_name,
    channel,
    start_date,
    end_date,
    loaded_at,
    _line
from cleaned
qualify row_number() over (
    partition by campaign_id
    order by loaded_at desc nulls last, start_date desc nulls last, _line desc
) = 1
