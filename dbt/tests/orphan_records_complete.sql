{{ config(severity = 'error') }}

-- Derive every reason directly from staging, independently of Silver flags.
with payment_relationships as (
    select
        p.payment_id,
        p.customer_id,
        p.subscription_id,
        c.customer_id as matched_customer_id,
        s.subscription_id as matched_subscription_id,
        s.customer_id as subscription_customer_id,
        sc.customer_id as matched_subscription_customer_id
    from {{ ref('stg_stripe__payments') }} as p
    left join {{ ref('stg_stripe__customers') }} as c on p.customer_id = c.customer_id
    left join {{ ref('stg_stripe__subscriptions') }} as s on p.subscription_id = s.subscription_id
    left join {{ ref('stg_stripe__customers') }} as sc on s.customer_id = sc.customer_id
),
expected as (
    select
        'salesforce' as source_system,
        'opportunity' as record_type,
        o.opportunity_id as record_id,
        o.account_id as parent_key,
        'missing_account' as exception_reason
    from {{ ref('stg_salesforce__opportunities') }} as o
    where not exists (
        select 1 from {{ ref('stg_salesforce__accounts') }} as a
        where a.account_id = o.account_id
    )

    union all

    select
        'stripe', 'subscription', s.subscription_id, s.customer_id, 'missing_customer'
    from {{ ref('stg_stripe__subscriptions') }} as s
    where not exists (
        select 1 from {{ ref('stg_stripe__customers') }} as c
        where c.customer_id = s.customer_id
    )

    union all

    select
        'stripe', 'payment', payment_id, customer_id, 'missing_customer'
    from payment_relationships
    where matched_customer_id is null

    union all

    select
        'stripe', 'payment', payment_id, subscription_id, 'missing_subscription'
    from payment_relationships
    where subscription_id is not null and matched_subscription_id is null

    union all

    select
        'stripe', 'payment', payment_id, subscription_id, 'orphaned_subscription'
    from payment_relationships
    where matched_subscription_id is not null and matched_subscription_customer_id is null

    union all

    select
        'stripe', 'payment', payment_id, subscription_id, 'subscription_customer_mismatch'
    from payment_relationships
    where matched_subscription_id is not null and not equal_null(customer_id, subscription_customer_id)

    union all

    select
        'adobe', 'event', event_id, cast(null as varchar), 'missing_event_timestamp'
    from {{ ref('stg_adobe__events') }}
    where event_timestamp is null

    union all

    select
        'stripe', 'customer', customer_id, cast(null as varchar), 'missing_created_at'
    from {{ ref('stg_stripe__customers') }}
    where created_at is null

    union all

    select
        'salesforce', 'account', account_id, cast(null as varchar), 'missing_updated_at'
    from {{ ref('stg_salesforce__accounts') }}
    where updated_at is null
),
expected_counts as (
    select
        sha2(to_json(array_construct(source_system, record_type, record_id, exception_reason)), 256) as exception_id,
        source_system, record_type, record_id, parent_key, exception_reason,
        count(*) as exception_count
    from expected
    group by source_system, record_type, record_id, parent_key, exception_reason
),
actual_counts as (
    select
        exception_id, source_system, record_type, record_id, parent_key, exception_reason,
        count(*) as exception_count
    from {{ ref('int_data_quality__orphan_records') }}
    group by exception_id, source_system, record_type, record_id, parent_key, exception_reason
),
missing as (
    select exception_id, source_system, record_type, record_id, parent_key, exception_reason, exception_count from expected_counts
    except
    select exception_id, source_system, record_type, record_id, parent_key, exception_reason, exception_count from actual_counts
),
unexpected as (
    select exception_id, source_system, record_type, record_id, parent_key, exception_reason, exception_count from actual_counts
    except
    select exception_id, source_system, record_type, record_id, parent_key, exception_reason, exception_count from expected_counts
)
select exception_id, source_system, record_type, record_id, parent_key, exception_reason, exception_count from missing
union all
select exception_id, source_system, record_type, record_id, parent_key, exception_reason, exception_count from unexpected
