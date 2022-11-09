{% materialization table, adapter='athena' -%}
  {%- set format = config.get('format', default='parquet') -%}
  {%- set table_type = config.get('table_type', default=none) -%}

  {%- set existing_relation = load_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {% set build_sql = materialize_table_iceberg(format, existing_relation, target_relation, sql)
      if table_type == 'iceberg' else materialize_table(format, existing_relation, target_relation, sql) %}

  {% call statement("main") %}
     {{ build_sql }}
  {% endcall %}

  {% if table_type == 'iceberg' %}
    -- Temp relation isn't used for non-iceberg tables
    {% set tmp_relation = make_temp_relation(target_relation) %}
    {% do adapter.drop_relation(tmp_relation) %}
  {% else %}
    -- Set custom table properties, not supported on iceberg tables
    {%- set table_properties = config.get('table_properties', default={}) -%}
    {%- do table_properties.update({'format': format}) -%}
    {{ set_table_properties(target_relation, table_properties) }}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
