-- A key must neither disappear from RAW nor be invented during staging.
{% set staging_models = [
    ('stripe_customers', 'customer_id'),
    ('stripe_subscriptions', 'subscription_id'),
    ('stripe_payments', 'payment_id'),
    ('salesforce_accounts', 'account_id'),
    ('salesforce_opportunities', 'opportunity_id'),
    ('adobe_events', 'event_id'),
    ('adobe_campaigns', 'campaign_id')
] %}

{% for entity, key in staging_models %}
    {% set source_name, entity_name = entity.split('_', 1) %}
    select
        '{{ entity }}' as entity,
        raw_keys.source_key,
        staged.{{ key }} as staged_key
    from (
        select distinct nullif(trim({{ key }}::varchar), '') as source_key
        from {{ source('raw', 'mock_' ~ entity) }}
    ) as raw_keys
    full outer join {{ ref('stg_' ~ source_name ~ '__' ~ entity_name) }} as staged
        on raw_keys.source_key = staged.{{ key }}
    where raw_keys.source_key is null or staged.{{ key }} is null
    {% if not loop.last %}union all{% endif %}
{% endfor %}
