{# 
  Герасимов. Макрос вызывается из модели <base_name>_union, ищет все версии базовой модели и создает по ним union-представление.
#}
{#
  debug run syntax:
  dbt run-operation make_union --args '{union_model_name: ms_terra01_cre_frtssupportwhatisdone_r1_union}'
#}
{# 
  regular use syntax:
  {{ make_union(this.identifier) }}
#}
{# Передаем имя запускаемой модели _union через {{ this }} макросу #}
{%- macro make_union(union_model_name) -%}
  {# Получаем базовое наименование сущности #}
  {% set model_base_name = union_model_name[:-6] %}
  {# {% do log(model_base_name , info=true) %} #}
  {# ФОрмируем шаблон для поиска версий сущности #}
  {% set model_search_pattern = model_base_name ~ '_v%' %}
  {# {% do log(model_search_pattern , info=true) %} #}
  {# Ищем все версии сущности по шаблону #}
  {% set model_versions = dbt_utils.get_relations_by_prefix('public', model_search_pattern) %}
  {# {% do log(model_versions , info=true) %} #}
  {# Формируем union all специальным макросом #}
  {{ dbt_utils.union_relations(relations = model_versions, source_column_name = None, ) }}
{%- endmacro -%}