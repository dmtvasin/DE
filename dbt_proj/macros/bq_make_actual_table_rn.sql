{# Бокоч. Макрос формирует BQ актуальный срез из поданной на вход таблицы отбором версий с максимальной датой внутри одного id #}
{# - p_source_name - имя source из sources.yml.
    Если передать 'null', макрос вместо source - будет использовать ref().
    Если передать реально существующее имя source - будет использовать source().
    Это позволяет строить a_ из s_ данным макросом #}
{# - p_source_table_name - имя таблицы-источника из sources.yml ИЛИ имя модели ДБТ #}
{# - p_id_column_name - имя PK таблицы, из которой строим этим макросом модель #}
{# - p_date_distinct_column_name - имя watermark-колонки таблицы, из которой строим этим макросом модель #}
{%- macro bq_make_actual_table ( p_source_name, p_source_table_name, p_id_column_name, p_date_distinct_column_name ) -%}

{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'delete_insert',
        partition_by = {
        'field': 'event_timestamp', 
        'data_type': 'timestamp',
        'granularity': 'day'
    },
        unique_key = 'MD5(CONCAT(CAST(event_name as text),CAST(user_pseudo_id as text),CAST(event_timestamp as text)))',
        distributed_by = 'event_timestamp',
        orientation = 'column',
        compresslevel = 1,  
        watermark_field = 'last_modified',
        on_schema_change='append_new_columns',  
    )
}}
with cte_events as (
    Select
    MD5(CONCAT(CAST({{ p_date_distinct_column_name }} as {{ dbt.type_string() }}),CAST({{ p_id_column_name }}  as {{ dbt.type_string() }}),CAST({{ p_date_distinct_column_name }} as {{ dbt.type_string() }}))) as id,
    ROW_NUMBER() OVER(PARTITION BY MD5(CONCAT(CAST(event_name as {{ dbt.type_string() }}),CAST({{ p_id_column_name }} as {{ dbt.type_string() }}),CAST({{ p_date_distinct_column_name }} as {{ dbt.type_string() }}))) ORDER BY event_timestamp desc) AS rn,
  --  MD5(CONCAT(CAST({{ p_date_distinct_column_name }} as text),CAST({{ p_id_column_name }}  as text),CAST({{ p_date_distinct_column_name }} as text))) as id,
  --  ROW_NUMBER() OVER(PARTITION BY MD5(CONCAT(CAST(event_name as text),CAST({{ p_id_column_name }} as text),CAST({{ p_date_distinct_column_name }} as text))) ORDER BY event_timestamp desc) AS rn,
    *
    FROM 
    {{source_or_ref(p_source_name, p_source_table_name) }} source_table
    where 1 = 1
    and last_modified >= '2023-01-01' 
)
Select 
    *
from cte_events
where 1 = 1
    and rn = 1 -- дедубликация по row_number с предыдущего шага
{% if is_incremental() %}
    and cte_events.{{ config.get('watermark_field') }} > (select max({{ config.get('watermark_field') }}) from {{ this }})
{%- endif -%}
{%- endmacro -%}





