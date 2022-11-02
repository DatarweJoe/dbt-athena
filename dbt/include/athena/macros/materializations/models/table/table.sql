{% materialization table, adapter='athena' -%}
  {%- set format = config.get('format', default='parquet') -%}
  {%- set iceberg = config.get('iceberg', default=False) -%}

  {%- set existing_relation = load_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {% set build_sql = materialize_table_iceberg(format, existing_relation, target_relation, sql)
      if iceberg else materialize_table(format, existing_relation, target_relation, sql) %}

  {% call statement("main") %}
     {{ build_sql }}
  {% endcall %}

  {% if iceberg %}
    {% set tmp_relation = make_temp_relation(target_relation) %}
    {% do adapter.drop_relation(tmp_relation) %}
  {% else %}
    {{ set_table_classification(target_relation, format) }}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
