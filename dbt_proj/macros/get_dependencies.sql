{# Герасимов. Макрос выдает плоский список sources и refs указанной модели #}
{# Если сорц или реф более одного раза - возвращает дубли, но допиливать пока не буду, ибо никто этот макрос не использует #}
{# Пример запуска из командной строки:
  dbt run-operation get_dependencies --args '{model_name: DimCarrier}'
  dbt run-operation get_dependencies --args '{model_name: a_fuel_fuelsupplyschemes}'
  dbt run-operation get_dependencies --args '{model_name: s_pg_crrpg01_tmstl_transportmileages}'
#}
{# Пример запуска из модели: неприменимо, поскольку ничего не возвращает и не печатает, кроме лога #}
{%- macro get_dependencies(model_name) -%}
    {% set models = graph.nodes.values() %}
    {% set model = (models | selectattr('name', 'equalto', model_name) | list).pop() %}
    {# {% do log("model: " ~ model, info=true) %} #}
    {# sources: #}
    {% set sources = model['sources'] %}
    {% do log("sources: ", info=true) %}
    {% for source in sources %}
      {% do log(source[0] ~ '.' ~ source[1], info=true) %}
    {% endfor %}
    {# refs: #}
    {% set refs_list = model['refs'] %}
    {% do log("refs: ", info=true) %}
    {% for dict_item in refs_list %}
      {% for key, value in dict_item.items() %}
        {% if key == "name" %}
          {% do log(value, info=true) %}
        {% endif %}
      {% endfor %}
    {% endfor %}
{%- endmacro -%}