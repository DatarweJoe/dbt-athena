{% materialization table, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}
  {%- set format = config.get('format', default='parquet') -%}
  {%- set iceberg = config.get('iceberg', default=False) -%}
  {%- set old_relation = adapter.get_relation(database=database, 
                                              schema=schema, 
                                              identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {{ materialize_table_iceberg(format, old_relation, target_relation, tmp_relation, sql) 
     if iceberg else materialize_table(format, old_relation, target_relation, sql) }}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
