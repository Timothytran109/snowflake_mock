-- Current-state data-quality snapshot; the historical table name is retained.
-- Grain: source system / record type / business key / exception reason.
-- This timestamp is the evaluation time, not historical first detection.
with exceptions as (
    select
        'salesforce' as source_system,
        'opportunity' as record_type,
        opportunity_id as record_id,
        account_id as parent_key,
        'missing_account' as exception_reason
    from {{ ref('int_salesforce__opportunities') }}
    where is_orphan

    union all

    select
        'stripe' as source_system,
        'subscription' as record_type,
        subscription_id as record_id,
        customer_id as parent_key,
        'missing_customer' as exception_reason
    from {{ ref('int_stripe__subscriptions') }}
    where is_orphan

    union all

    select
        'stripe' as source_system,
        'payment' as record_type,
        payment_id as record_id,
        customer_id as parent_key,
        'missing_customer' as exception_reason
    from {{ ref('int_stripe__payments') }}
    where is_orphan and is_customer_orphan

    union all

    select
        'stripe' as source_system,
        'payment' as record_type,
        payment_id as record_id,
        subscription_id as parent_key,
        'missing_subscription' as exception_reason
    from {{ ref('int_stripe__payments') }}
    where is_orphan and is_subscription_missing

    union all

    select
        'stripe' as source_system,
        'payment' as record_type,
        payment_id as record_id,
        subscription_id as parent_key,
        'orphaned_subscription' as exception_reason
    from {{ ref('int_stripe__payments') }}
    where is_orphan and is_subscription_orphan

    union all

    select
        'stripe' as source_system,
        'payment' as record_type,
        payment_id as record_id,
        subscription_id as parent_key,
        'subscription_customer_mismatch' as exception_reason
    from {{ ref('int_stripe__payments') }}
    where is_orphan and is_subscription_customer_mismatch

    union all

    select
        'adobe' as source_system,
        'event' as record_type,
        event_id as record_id,
        cast(null as varchar) as parent_key,
        'missing_event_timestamp' as exception_reason
    from {{ ref('stg_adobe__events') }}
    where event_timestamp is null

    union all

    select
        'stripe' as source_system,
        'customer' as record_type,
        customer_id as record_id,
        cast(null as varchar) as parent_key,
        'missing_created_at' as exception_reason
    from {{ ref('stg_stripe__customers') }}
    where created_at is null

    union all

    select
        'salesforce' as source_system,
        'account' as record_type,
        account_id as record_id,
        cast(null as varchar) as parent_key,
        'missing_updated_at' as exception_reason
    from {{ ref('stg_salesforce__accounts') }}
    where updated_at is null
)

select
    sha2(to_json(array_construct(source_system, record_type, record_id, exception_reason)), 256) as exception_id,
    source_system,
    record_type,
    record_id,
    parent_key,
    '{{ run_started_at.isoformat() }}'::timestamp_tz as detected_at,
    exception_reason
from exceptions
