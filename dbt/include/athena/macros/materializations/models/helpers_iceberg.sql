{% macro drop_iceberg(relation) -%}
  drop table if exists {{ relation }}
{% endmacro %}

{% macro create_table_iceberg(relation, old_relation, tmp_relation, sql) -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set staging_location = config.get('staging_location') -%}
  {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
  {%- set bucketed_by = config.get('bucketed_by', default=none) -%}
  {%- set bucket_count = config.get('bucket_count', default=none) -%}
  {%- set write_compression = config.get('write_compression', default=none) -%}

  {%- set target_relation = this.incorporate(type='table') -%}

  {% if tmp_relation is not none %}
     {% do adapter.drop_relation(tmp_relation) %}
  {% endif %}

  -- create tmp table
  {% do run_query(create_tmp_table_iceberg(tmp_relation, sql, staging_location)) %}

  -- get columns from tmp table to retrieve metadata
  {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}

  -- drop old relation after tmp table is ready
  {%- if old_relation is not none -%}
  	{% do run_query(drop_iceberg(old_relation)) %}
  {%- endif -%}

  -- create iceberg table
  {% do run_query(create_iceberg_table_definition(target_relation, dest_columns)) %}

  -- return final insert statement
  {{ return(incremental_insert(tmp_relation, target_relation)) }}

{% endmacro %}


{% macro create_tmp_table_iceberg(relation, sql, staging_location) -%}
  create table
    {{ relation }}

    with (
        write_compression='snappy',
        format='parquet'
    )
  as
    {{ sql }}
{% endmacro %}

{% macro create_iceberg_table_definition(relation, dest_columns) -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set strict_location = config.get('strict_location', default=true) -%}
  {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
  {%- set table_properties = config.get('table_properties', default={}) -%}
  {%- set _ = table_properties.update({'table_type': 'ICEBERG'}) -%}
  {%- set table_properties_formatted = [] -%}
  {%- set dest_columns_with_type = [] -%}

  {%- for k in table_properties -%}
  	{% set _ = table_properties_formatted.append("'" + k + "'='" + table_properties[k] + "'") -%}
  {%- endfor -%}

  {%- set table_properties_csv= table_properties_formatted | join(', ') -%}

  {%- set external_location = adapter.get_unique_external_location(external_location, strict_location, target.s3_staging_dir, relation.name) -%}

  {%- for col in dest_columns -%}
	{% set dtype = iceberg_data_type(col.dtype) -%}
  	{% set _ = dest_columns_with_type.append(col.name + ' ' + dtype) -%}
  {%- endfor -%}

  {%- set dest_columns_with_type_csv = dest_columns_with_type | join(', ') -%}

  CREATE TABLE {{ relation }} (
    {{ dest_columns_with_type_csv }}
  )
  {%- if partitioned_by is not none %}
    {%- set partitioned_by_csv = partitioned_by | join(', ') -%}
  	PARTITIONED BY ({{partitioned_by_csv}})
  {%- endif %}
  LOCATION '{{ external_location }}'
  TBLPROPERTIES (
  	{{table_properties_csv}}
  )

{% endmacro %}

{% macro iceberg_data_type(athena_type) -%}
    -- TODO: add support for complex data types
    {% set athena_to_iceberg_type_map = ({
      -- Mappings pulled from https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-supported-data-types.html
      "boolean": "boolean", 
      "tinyint": "int", 
      "smallint": "int", 
      "int": "int", 
      "bigint": "long", 
      "double": "double",
      "float": "float",
      "char": "string",
      "string": "string",
      "varchar": "string",
      "binary": "binary",
      "date": "date",
      "timestamp": "timestamp",
      "timestamptz": "timestamp"}) %}
    {% set iceberg_type = athena_to_iceberg_type_map[athena_type] %}
    {{ return(iceberg_type) }}    
{% endmacro %}
