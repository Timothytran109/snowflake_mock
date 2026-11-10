{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Only the verified local test profile maps marts to the shared GOLD schema.
       Other targets retain dbt's default schema isolation behavior. #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'dev'
        and target.database | upper == 'CUSTOMER360_TEST'
        and target.schema | upper == 'SILVER'
        and custom_schema_name | trim | upper == 'GOLD' -%}
        GOLD
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
