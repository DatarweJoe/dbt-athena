{% macro materialize_table(format, existing_relation, target_relation, sql) -%}
    {%- if existing_relation is not none -%}
        {{ adapter.drop_relation(existing_relation) }}
    {%- endif -%}

    -- Generate and execute build SQL
    {%- set build_sql = create_table_as(False, target_relation, sql) -%}

    {% call statement("main") %}
        {{ build_sql }}
    {% endcall %}

    {{ set_table_classification(target_relation, format) }}
{%- endmacro %}

{% macro materialize_table_iceberg(format, existing_relation, target_relation, sql) -%}
    {{ validate_format_iceberg(format) }}

    {%- set build_sql = create_table_iceberg(existing_relation, target_relation, sql) -%}

    {% call statement("main") %}
      {{ build_sql }}
    {% endcall %}

    {% do adapter.drop_relation(tmp_relation) %}
{%- endmacro %}