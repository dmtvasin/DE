{# Герасимов. Макрос формирует актуальный срез из поданной на вход таблицы отбором версий с максимальной датой внутри одного id #}
{# - p_source_name - имя source из sources.yml.
    Если передать 'null', макрос вместо source - будет использовать ref().
    Если передать реально существующее имя source - будет использовать source().
    Это позволяет строить a_ из s_ данным макросом #}
{# - p_source_table_name - имя таблицы-источника из sources.yml ИЛИ имя модели ДБТ #}
{# - p_id_column_name - имя PK таблицы, из которой строим этим макросом модель #}
{# - p_date_distinct_column_name - имя watermark-колонки таблицы, из которой строим этим макросом модель #}
{%- macro make_actual_table_rn ( p_source_name, p_source_table_name, p_id_column_name, p_date_distinct_column_name ) -%}
  select *
  from 
    (
    select
      *,
      row_number() over(partition by {{ p_id_column_name }} order by {{ p_date_distinct_column_name }} desc ) as rn
    from
      {{ source_or_ref(p_source_name, p_source_table_name) }}
      {%- if is_incremental() %}
      where {{ p_date_distinct_column_name }} > cast( {{ n_days_diff(-7) }} as {{ type_timestamp() }} ) {# отсекаем старые данные, чтобы их не лопатить #}
      {%- endif -%}
    )
    max_table
  where 1 = 1
    and rn = 1
{%- if is_incremental() %}
    and max_table.{{ p_date_distinct_column_name }} > cast( {{ n_days_diff(-7) }} as {{ type_timestamp() }} )
{%- endif -%}
{%- endmacro -%}