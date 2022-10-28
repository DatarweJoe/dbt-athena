{% macro validate_get_incremental_strategy(raw_strategy) %}
  {% set valid_strategies = ['append', 'insert_overwrite'] %}
  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: {{ valid_strategies | map(attribute='quoted') | join(', ') }}
  {%- endset %}

  {% if raw_strategy not in valid_strategies %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(raw_strategy) %}
{% endmacro %}

{% macro validate_get_unique_key(unique_key) %}
    {% set overwrite_msg -%}
      Athena adapter does not support 'unique_key'
    {%- endset %}
    {% if unique_key is not none %}
      {% do exceptions.raise_compiler_error(overwrite_msg) %}
    {% endif %}
{%- endmacro %}

{% macro generate_incremental_insert_query(tmp_relation, target_relation, statement_name="main") %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    );
{%- endmacro %}


{% macro get_partitions(partition_cols) %}
  {%- set partitioned_keys = list_to_csv(partitioned_by) -%}
  {% call statement('get_partitions', fetch_result=True) %}
    select distinct {{partitioned_keys}} from {{ tmp_relation }};
  {% endcall %}
  {%- set table = load_result('get_partitions').table -%}
  {% do return(table) %}
{% endmacro %}


{% macro get_partition_expressions(relation, partition_cols) %}
  {% set distinct_partitions = get_partitions(partition_cols) %}
  {%- set rows = distinct_partitions.rows -%}
  {%- set partitions = [] -%}
  {%- for row in rows -%}
    {%- set single_partition = [] -%}
    {%- for col in row -%}
      {%- set column_type = adapter.convert_type(distinct_partitions, loop.index0) -%}
      {%- if column_type == 'integer' -%}
        {%- set value = col|string -%}
      {%- elif column_type == 'string' -%}
        {%- set value = "'" + col + "'" -%}
      {%- elif column_type == 'date' -%}
        {%- set value = "'" + col|string + "'" -%}
      {%- else -%}
        {%- do exceptions.raise_compiler_error('Need to add support for column type ' + column_type) -%}
      {%- endif -%}
      {%- do single_partition.append(partitioned_by[loop.index0] + '=' + value) -%}
    {%- endfor -%}
    {%- set single_partition_expression = single_partition | join(' and ') -%}
    {%- do partitions.append('(' + single_partition_expression + ')') -%}
    {%- do return(partitions) -%}
  {%- endfor -%}
{% endmacro %}

{% macro delete_overlapping_partitions(target_relation, tmp_relation, partitioned_by, iceberg = False) %}
  {%- set partition_expressions = get_partition_expressions(tmp_relation, partitioned_by) -%}
  {%- if (partition_expressions | length) > 100 -%}
    {% set error_message %}
      A maximum of one-hundred (100) partitions can be written to by a single query.
    {% endset %}
    {% do exceptions.raise_compiler_error(error_message) %}
  {%- endif -%}
  {%- set full_partition_expression = partition_expressions | join(' or ') -%}
  {% if iceberg %}
    {%- set delete_partition_data_statement -%}
      DELETE FROM {{ target_relation }}
      WHERE {{ full_partition_expression }}
    {%- endset %}
    {%- do run_query(delete_partition_data_statement)}
  {% else %}
    {%- do adapter.clean_up_partitions(target_relation.schema, target_relation.table, full_partition_expression) -%}
  {-% endif -%}
{%- endmacro %}
