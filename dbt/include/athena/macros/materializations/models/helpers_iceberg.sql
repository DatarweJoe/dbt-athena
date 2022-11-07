{% macro drop_iceberg(relation) -%}
  drop table if exists {{ relation }}
{% endmacro %}

{% macro create_table_iceberg(existing_relation, target_relation, sql) -%}
  {%- set tmp_relation = materialize_temp_relation(target_relation, sql) -%}

  -- get columns from tmp table to retrieve metadata
  {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}

  -- drop old relation after tmp table is ready
  {%- if existing_relation is not none -%}
  	{% do run_query(drop_iceberg(existing_relation)) %}
  {%- endif -%}

  -- create iceberg table
  {% do run_query(create_iceberg_table_definition(target_relation, dest_columns)) %}

  -- return final insert statement
  {{ return(generate_incremental_insert_query(tmp_relation, target_relation)) }}
{% endmacro %}

{% macro create_iceberg_table_definition(relation, dest_columns) -%}
  -- TODO: add support for bucketing
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
  
  {%- set create_table_query -%}
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
  {%- endset -%}
  {%- do return(create_table_query) -%}
{% endmacro %}

{% macro iceberg_data_type(athena_type) -%}
    -- TODO: add support for complex data types
    -- Mappings pulled from https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-supported-data-types.html
    {% set athena_to_iceberg_type_map = ({
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

{% macro validate_format_iceberg(format) -%}
    {% set valid_formats = ['parquet'] %}
    {% set invalid_iceberg_format_msg -%}
        Invalid format provided for iceberg table: {{ format }}
        Expected one of: {{ valid_formats | map(attribute='quoted') | join(', ') }}
    {%- endset %}
    {%- if format not in valid_formats -%}
        {% do exceptions.raise_compiler_error(invalid_iceberg_format_msg) %}
    {%- endif -%}  
{% endmacro %}