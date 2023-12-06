{# 
  Герасимов. Макрос возвращает true или false в зависимости от того, существует в СУБД указанная таблица или нет.
#}
{#
  debug run syntax:
  dbt run-operation table_exists --args '{schema_name: snapshots, table_name: s_creatio_contract}'
  dbt run-operation table_exists --args '{schema_name: snapshots, table_name: s_creatio_contract111}'
#}
{# 
  regular use syntax:
  {{ table_exists('snapshots', 's_creatio_contract') }}
  {{ table_exists(this.schema, this.identifier) }}
#}
{%- macro table_exists(schema_name, table_name) -%}
  {% set models_found = dbt_utils.get_relations_by_prefix(schema_name, table_name) %}
  {% if models_found|length > 0 %}
    {# {% do log(models_found , info=true) %} #}
    {{ return(true) }}
  {% else %}
    {# {% do log(models_found , info=true) %} #}
    {{ return(false) }}
  {% endif %}
{%- endmacro -%}