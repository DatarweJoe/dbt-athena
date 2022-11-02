{% macro materialize_table(format, existing_relation, target_relation, sql) -%}
    {%- if existing_relation is not none -%}
        {{ adapter.drop_relation(existing_relation) }}
    {%- endif -%}

    -- Generate and execute build SQL
    {%- set build_sql = create_table_as(False, target_relation, sql) -%}

    {% do return(build_sql) %}
{%- endmacro %}

{% macro materialize_table_iceberg(format, existing_relation, target_relation, sql) -%}
    {{ validate_format_iceberg(format) }}

    {%- set build_sql = create_table_iceberg(existing_relation, target_relation, sql) -%}

    {% do return(build_sql) %}
{%- endmacro %}