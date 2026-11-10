-- Optional dates may be absent, but nonblank input must not silently parse to null.
{% set optional_dates = [
    ('mock_stripe_subscriptions', 'stg_stripe__subscriptions', 'subscription_id', 'end_date'),
    ('mock_salesforce_opportunities', 'stg_salesforce__opportunities', 'opportunity_id', 'closed_at'),
    ('mock_adobe_campaigns', 'stg_adobe__campaigns', 'campaign_id', 'end_date')
] %}

{% for table, model, key, column in optional_dates %}
    select
        '{{ table }}' as source_table,
        raw.{{ key }}::varchar as source_key,
        '{{ column }}' as date_column
    from {{ source('raw', table) }} as raw
    inner join {{ ref(model) }} as staged
        on raw._line = staged._line
        and nullif(trim(raw.{{ key }}::varchar), '') = staged.{{ key }}
    where (
        nullif(trim(raw.{{ column }}::varchar), '') is not null
        and staged.{{ column }} is null
    ) or (
        nullif(trim(raw.{{ column }}::varchar), '') is null
        and staged.{{ column }} is not null
    )
    {% if not loop.last %}union all{% endif %}
{% endfor %}
