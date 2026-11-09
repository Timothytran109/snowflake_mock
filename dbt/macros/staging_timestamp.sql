{% macro staging_timestamp(column_expression) -%}
    {# RAW mock exports use ISO, US month/day/year, and year/month/day dates.
       TIMESTAMP_NTZ preserves the source wall-clock time; no timezone is inferred. #}
    {% set value = "nullif(trim(" ~ column_expression ~ "::varchar), '')" %}
    coalesce(
        try_to_timestamp_ntz({{ value }}, 'YYYY-MM-DD HH24:MI:SS.FF'),
        try_to_timestamp_ntz({{ value }}, 'YYYY-MM-DD HH24:MI:SS'),
        try_to_timestamp_ntz({{ value }}, 'YYYY-MM-DD'),
        try_to_timestamp_ntz({{ value }}, 'MM/DD/YYYY HH24:MI:SS'),
        try_to_timestamp_ntz({{ value }}, 'MM/DD/YYYY HH24:MI'),
        try_to_timestamp_ntz({{ value }}, 'MM/DD/YYYY'),
        try_to_timestamp_ntz({{ value }}, 'YYYY/MM/DD HH24:MI:SS'),
        try_to_timestamp_ntz({{ value }}, 'YYYY/MM/DD')
    )
{%- endmacro %}
