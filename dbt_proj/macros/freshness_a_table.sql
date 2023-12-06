{# Сумова. Тест определяет свежесть данных, сравнивая последнюю дату из а-таблицы "model" и последнюю дату вчерашнего дня таблицы источника "compare_model" 
  с возможностью добавить фильтр и дату отсечки
#}
{%- macro test_freshness_a_table(model, compare_model, time_column, view_date, filter) -%}
  {% if execute %}
    {# Достаем интервал временного сдвига из метаданных в run_query() #}
    {% set metadata_query %}
      select 
        substr(partition_column_convert, {{ position("'interval'", "partition_column_convert") }}) as partition_column_convert_part
      from {{ source('gp_public', 'pg_set_stg_command') }} as c
      where 1=1
        and name = regexp_replace('{{ get_one_source_by_model_name(model.identifier) }}', '\W+', '', 'g')
        and date(load_dttm) = {{ view_date }}
        and partition_column = '{{ time_column }}'
    {% endset %}
    {% set metadata_query_results = run_query(metadata_query) %}
      {% if metadata_query_results.columns[0].values()[0] %}
        {% set partition_column_convert_interval = '+ ' + metadata_query_results.columns[0].values()[0] %}
      {% else %}
        {% set partition_column_convert_interval = "" %}
      {% endif %}
  {% endif %}
  with
  dwh_table as
    (
    select 
      1 as id_test_freshness,
      max({{ time_column }}) as max_date_dwh_table
    from {{ model }}
    ),
  ext_table as
  (
  select 
    1 as id_test_freshness,
    max("{{ time_column }}") as max_date_ext_table
  from {{ compare_model }}
  where 1=1
    and "{{ time_column }}" {{ partition_column_convert_interval }} < {{ view_date }}
    {{ filter }}
  ),
  final as
    (
    select
      max_date_dwh_table,
      max_date_ext_table
    from dwh_table
    full join ext_table
      on dwh_table.id_test_freshness = ext_table.id_test_freshness
    where {{ dbt.date_trunc("second", "max_date_dwh_table") }} < {{ dbt.date_trunc("second", "max_date_ext_table") }}
    )
  select * from final
{%- endmacro -%}
