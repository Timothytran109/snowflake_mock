{% test same_rows(model, compare_model, columns, compare_where='true') %}
-- Compare all fact values in both directions, not just counts or sums.
-- Paired primary-key tests catch duplicates that set comparison cannot detect.
with expected as (
    select {{ columns | join(', ') }}
    from {{ compare_model }}
    where {{ compare_where }}
),
actual as (
    select {{ columns | join(', ') }}
    from {{ model }}
),
missing as (
    select {{ columns | join(', ') }} from expected
    except
    select {{ columns | join(', ') }} from actual
),
unexpected as (
    select {{ columns | join(', ') }} from actual
    except
    select {{ columns | join(', ') }} from expected
)
select {{ columns | join(', ') }} from missing
union all
select {{ columns | join(', ') }} from unexpected
{% endtest %}
