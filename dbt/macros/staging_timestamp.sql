{% macro staging_timestamp(column_expression) -%}
    {# Preserve source wall-clock time as TIMESTAMP_NTZ.
       Explicit offsets are accepted but are not normalized to UTC.
       Blank and unsupported values remain null. #}
    {% set value = "nullif(trim(" ~ column_expression ~ "::varchar), '')" %}
    {% set formats = [
        'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"',
        'YYYY-MM-DD"T"HH24:MI:SS"Z"',
        'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM',
        'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM',
        'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM',
        'YYYY-MM-DD HH24:MI:SS TZH:TZM',
        'YYYY-MM-DD HH24:MI:SS.FF TZHTZM',
        'YYYY-MM-DD HH24:MI:SS TZHTZM',
        'YYYY-MM-DD HH24:MI:SS.FF',
        'YYYY-MM-DD HH24:MI:SS',
        'YYYY-MM-DD',
        'MM/DD/YYYY HH24:MI:SS',
        'MM/DD/YYYY HH24:MI',
        'MM/DD/YYYY',
        'YYYY/MM/DD HH24:MI:SS',
        'YYYY/MM/DD'
    ] %}
    coalesce(
        {% for format in formats %}
        try_to_timestamp_ntz({{ value }}, '{{ format }}'){% if not loop.last %},{% endif %}
        {% endfor %}
    )
{%- endmacro %}
