{{ config(severity = 'error') }}

-- Independently derive the expected exceptions directly from staging.
with expected as (
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
        'stripe', 'payment', p.payment_id,
        case when c.customer_id is null then p.customer_id else p.subscription_id end,
        case
            when c.customer_id is null then 'missing_customer'
            when s.subscription_id is null then 'missing_subscription'
            when sc.customer_id is null then 'orphaned_subscription'
            else 'subscription_customer_mismatch'
        end
    from {{ ref('stg_stripe__payments') }} as p
    left join {{ ref('stg_stripe__customers') }} as c on p.customer_id = c.customer_id
    left join {{ ref('stg_stripe__subscriptions') }} as s on p.subscription_id = s.subscription_id
    left join {{ ref('stg_stripe__customers') }} as sc on s.customer_id = sc.customer_id
    where c.customer_id is null
        or (p.subscription_id is not null and (
            s.subscription_id is null or sc.customer_id is null
            or not equal_null(p.customer_id, s.customer_id)
        ))
),
actual as (
    select source_system, record_type, record_id, parent_key, exception_reason
    from {{ ref('int_data_quality__orphan_records') }}
),
missing as (
    select source_system, record_type, record_id, parent_key, exception_reason from expected
    except
    select source_system, record_type, record_id, parent_key, exception_reason from actual
),
unexpected as (
    select source_system, record_type, record_id, parent_key, exception_reason from actual
    except
    select source_system, record_type, record_id, parent_key, exception_reason from expected
)
select source_system, record_type, record_id, parent_key, exception_reason from missing
union all
select source_system, record_type, record_id, parent_key, exception_reason from unexpected
