{# Сумова. Тест определяет кол-во уникальных значений в указанных столбцах из а-таблицы "model" и таблицы источника "compare_model" с возможностью добавить фильтр #}
{% macro test_equal_distinct_rowcount(model, compare_model, view_column, time_column, view_date, filter) %}

with
dwh_table as
  (
  select 
    1 as id_test_equal_distinct_rowcount,
    count(distinct {{ view_column }}) as distinct_rowcount_dwh_table
  from {{ model }}
    where 1=1
      and "{{ time_column }}" < {{ view_date }}
  ),

ext_table as (

    select 
      1 as id_test_equal_distinct_rowcount,
      count(distinct "{{ view_column }}") as distinct_rowcount_ext_table
    from {{ compare_model }}
    where 1=1
      and "{{ time_column }}" < {{ view_date }}
      {{ filter }}

),

final as
  (
  select
    distinct_rowcount_dwh_table,
    distinct_rowcount_ext_table
  from dwh_table
  full join ext_table
    on dwh_table.id_test_equal_distinct_rowcount = ext_table.id_test_equal_distinct_rowcount
  where distinct_rowcount_dwh_table <> distinct_rowcount_ext_table
  )

select * from final
{%- endmacro -%}