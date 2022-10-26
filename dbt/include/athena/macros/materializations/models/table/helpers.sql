{% macro materialize_table(format, old_relation, target_relation, sql) -%}
    {%- if old_relation -%}
        {{ adapter.drop_relation(old_relation) }}
    {%- endif -%}

    -- Generate and execute build SQL
    {%- set build_sql = create_table_as(False, target_relation, sql) -%}

    {% call statement("main") %}
        {{ build_sql }}
    {% endcall %}

    {{ set_table_classification(target_relation, format) }}
{%- endmacro %}

{% macro materialize_table_iceberg(format, old_relation, target_relation, tmp_relation, sql) -%}
    -- If iceberg is True, ensure that a valid format is provided.
    {% set invalid_iceberg_format_msg -%}
        Invalid format provided for iceberg table: {{ format }}
        Expected one of: 'parquet'
    {%- endset %}
    {%- if format != 'parquet' -%}
        {% do exceptions.raise_compiler_error(invalid_iceberg_format_msg) %}
    {%- endif -%}

    {%- set tmp_relation = make_temp_relation(target_relation) -%}

    {%- set build_sql = create_table_iceberg(target_relation, old_relation, tmp_relation, sql) -%}

    {% call statement("main") %}
      {{ build_sql }}
    {% endcall %}

    {% do adapter.drop_relation(tmp_relation) %}
{%- endmacro %}