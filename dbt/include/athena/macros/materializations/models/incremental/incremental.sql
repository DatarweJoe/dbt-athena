{% materialization incremental, adapter='athena' -%}
  
  {% set unique_key = config.get('unique_key') %}
  {{ validate_get_unique_key(unique_key) }}

  {% set strategy = config.get('incremental_strategy', default='insert_overwrite') %}
  {{ validate_get_incremental_strategy(strategy) }}

  {%- set table_type = config.get('table_type') | lower -%}

  {% set format = config.get('format', default='parquet') %}
  {% set partitioned_by = config.get('partitioned_by', default=none) %}
  
  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% if existing_relation is none %}
      -- If the relation doesn't exist we build from scratch.
      -- Athena's iceberg implementation doesn't currently support CTAS so we
      -- need to use a temp table as a workaround.
      {% set build_sql = materialize_table_iceberg(format, existing_relation, target_relation, sql) 
         if table_type == 'iceberg' else materialize_table(format, existing_relation, target_relation, sql) %}
  {% elif existing_relation.is_view %}
      -- If the relation exists as a view drop it and build from scratch.
      {% do adapter.drop_relation(existing_relation) %}
      {% set build_sql = materialize_table_iceberg(format, existing_relation, target_relation, sql) 
         if table_type == 'iceberg' else materialize_table(format, existing_relation, target_relation, sql) %}
  {% elif should_full_refresh() %}
      -- If we're running a full refresh drop the existing relation and build
      -- from scratch.
      {% if table_type == 'iceberg' %}
        -- Iceberg tables are managed tables in Athena so dropping one
        -- automatically removes data from s3, no need to handle this.
        {% do run_query(drop_iceberg(existing_relation)) %}
        {% set build_sql = materialize_table_iceberg(format, existing_relation, target_relation, sql) %}
      {% else %}
        {% do adapter.drop_relation(existing_relation) %}
        {% set build_sql = materialize_table(format, existing_relation, target_relation) %}
        --{% set build_sql = create_table_as(False, target_relation, sql) %}
      {% endif %}
  {% elif partitioned_by is not none and strategy == 'insert_overwrite' %}
      -- The table exists, is partitioned and we're using an insert_overwrite
      -- strategy, clean up existing partitions and run an incremental insert.
      {% set tmp_relation = materialize_temp_relation(target_relation, sql) %}
      {% do delete_overlapping_partitions(target_relation, tmp_relation, partitioned_by, table_type=table_type) %}
      {% set build_sql = generate_incremental_insert_query(tmp_relation, target_relation) %}
  {% else %}
      -- The table exists and is not partitioned or we're using an append
      -- strategy, run an incremental insert.
      {% set tmp_relation = materialize_temp_relation(target_relation, sql) %}
      {% set build_sql = generate_incremental_insert_query(tmp_relation, target_relation) %}
  {% endif %}

  -- Execute the build sql
  {% call statement("main") %}
    {{ build_sql }}
  {% endcall %}

  -- If a temp table was used then drop it 
  {% set tmp_relation = make_temp_relation(target_relation) %}
  {% if tmp_relation is not none %}
     {% do adapter.drop_relation(tmp_relation) %}
  {% endif %}

  -- Set table properties
  {%- if table_type != 'iceberg' -%}
    {%- set table_properties = config.get('table_properties', default={}) -%}
    {%- do table_properties.update({'format': format}) -%}
    {{ set_table_properties(target_relation, table_properties) }}
  {%- endif -%}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
