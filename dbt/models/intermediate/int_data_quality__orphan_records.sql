-- Current-state exception snapshot, one row per source system/type/business key.
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
        case when is_customer_orphan then customer_id else subscription_id end as parent_key,
        case
            when is_customer_orphan then 'missing_customer'
            when is_subscription_missing then 'missing_subscription'
            when is_subscription_orphan then 'orphaned_subscription'
            when is_subscription_customer_mismatch then 'subscription_customer_mismatch'
        end as exception_reason
    from {{ ref('int_stripe__payments') }}
    where is_orphan
)

select
    sha2(to_json(array_construct(source_system, record_type, record_id)), 256) as exception_id,
    source_system,
    record_type,
    record_id,
    parent_key,
    '{{ run_started_at.isoformat() }}'::timestamp_tz as detected_at,
    exception_reason
from exceptions
