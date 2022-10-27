{% materialization table, adapter='athena' -%}
  {%- set format = config.get('format', default='parquet') -%}
  {%- set iceberg = config.get('iceberg', default=False) -%}

  {%- set existing_relation = adapter.load_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {{ materialize_table_iceberg(format, existing_relation, target_relation, sql) 
     if iceberg else materialize_table(format, existing_relation, target_relation, sql) }}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
