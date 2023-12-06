{# Герасимов. Макрос возвращает ОДИН источник для модели по ее имени в код #}
{# Пример запуска из командной строки:
  dbt run-operation get_one_source_by_model_name --args '{model_name: a_fuel_fuelsupplyschemes}'
  dbt run-operation get_one_source_by_model_name --args '{model_name: a_creatio_frtscargoloadingregions}'
#}
{# Пример запуска из модели: 
  select '{{ get_one_source_by_model_name('a_fuel_fuelsupplyschemes') }}' as source_name
#}
{%- macro get_one_source_by_model_name(model_name) -%}
    {%- if execute -%}
      {# {{ log('model_name: ' ~ model_name, info=True) }} #}
      {# {% set counter = counter + 1 %} #}
      {%- set models = graph.nodes.values() -%}
      {%- set model = (models | selectattr('name', 'equalto', model_name) | list).pop() -%}
      {# {% do log("model: " ~ model, info=true) %} #}
      {# sources: #}
      {%- set sources = model['sources'] -%}
      {%- set sources_length = sources|length -%}
      {# {% do log("0: sources: " ~ sources_length, info=true) %} #}
      {%- set refs_list = model['refs'] -%}
      {%- set refs_list_length = refs_list|length -%}
      {# {% do log("1: refs_list: " ~ refs_list_length, info=true) %} #}
      {# Если есть сорц - возвращаем его и не идем дальше #}
      {%- if sources_length > 0 -%}
        {%- for source in sources -%}
          {# {%- do log('get_one_source_by_model_name: ' ~ source[1], info=true) -%} #}
          {{- return(source[1]|trim()) -}}
        {%- endfor -%}
      {%- endif -%}
      {# Есть только рефы, а сорцов нет - ищем дальше #}
      {%- if refs_list_length > 0 and sources_length == 0 -%}
        {# {% do log('2 if1: ', info=true) %} #}
        {%- for dict_item in refs_list -%}
          {%- for key, value in dict_item.items() -%}
            {%- if key == "name" -%}
              {# {% do log(' 4 ref: ' ~ value, info=true) %} #}
              {# Вызываем макрос рекурсивно, чтобы найти source найденного ref #}
              {{ get_one_source_by_model_name(value) }}
            {%- endif -%}
          {%- endfor -%}
        {%- endfor -%}
      {# Если оба множества пустые - мы дошли до конца графа и можно возвращать последний source #}
      {%- elif refs_list_length == 0 and sources_length == 0 -%}
        {# Получаем базовое наименование сущности #}
        {%- set model_base_name = model_name[:-6] -%}
        {# {%- do log('get_one_source_by_model_name: ' ~ model_base_name, info=true) -%} #}
        {{- return(model_base_name|trim()) -}}
      {%- endif -%}
    {%- endif -%}
{%- endmacro -%}