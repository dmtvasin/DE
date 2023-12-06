{# 
  Герасимов. Макрос генерирует снимок с заданной таблицы по указаному row_number. Нужен для начальной загрузки снапшотов (исторических таблиц).
#}
{#
  debug run syntax:
  dbt run-operation snapshot_initial --args '{snapshot_base_table_name: a_creatio_contract, rn_var: 11}'
#}
{# 
  regular use syntax:
  {{ snapshot_initial('a_creatio_contract', 10) }}
#}
{%- macro snapshot_initial(snapshot_base_table_name, rn_var) -%}
    select *
    from
      (
        select
          *,
          row_number() over(partition by id order by ModifiedOn asc ) as rn
            from {{ ref('u_' ~ snapshot_base_table_name) }}
      ) t
    where rn = {{ rn_var }}
{%- endmacro -%}