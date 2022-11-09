{% macro set_table_classification(relation, default_value) -%}
    {%- set format = config.get('format', default=default_value) -%}

    {% call statement('set_table_classification', auto_begin=False) -%}
        alter table {{ relation }} set tblproperties ('classification' = '{{ format }}')
    {%- endcall %}
{%- endmacro %}

{% macro materialize_temp_relation(target_relation, sql) -%}
    -- Materialize a query into a temp table
    {% set tmp_relation = make_temp_relation(target_relation) %}
    {% if tmp_relation is not none %}
        {% do adapter.drop_relation(tmp_relation) %}
    {% endif %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {{ return(tmp_relation) }}
{%- endmacro %}

{% macro list_to_csv(list) %}
  {{ return(list | join(', ')) }}
{%- endmacro %}

{%- macro set_table_properties(relation, table_properties) -%}
    {%- set table_properties_formatted = [] -%}
    {%- for k in table_properties -%}
        {% set _ = table_properties_formatted.append("'" + k|string + "'='" + table_properties[k]|string + "'") -%}
    {%- endfor -%}
    {%- set table_properties_csv = table_properties_formatted | join(', ') -%}
    {%- set alter_table_query -%}
        ALTER TABLE {{ relation }} SET TBLPROPERTIES ({{ table_properties_csv }})
    {%- endset -%}
    {%- do run_query(alter_table_query) -%}
{%- endmacro -%}