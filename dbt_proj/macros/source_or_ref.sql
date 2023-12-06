{# Герасимов. Макрос для использования в макросе make_actual_table для клмпактности #}
{# Получает на вход параметры макроса make_actual_table p_source_name и p_source_table_name #}
{# Если p_source_name = 'null' то возвращает {{ ref(p_source_table_name) }} #}
{# Если p_source_name != 'null' то возвращает {{ source(p_source_name, p_source_table_name) }} #}
{%- macro source_or_ref(p_source_name, p_source_table_name) -%}
    {% if p_source_name == 'null' %}
        {{ ref(p_source_table_name) }}
    {% else %}
        {{ source(p_source_name, p_source_table_name) }}
    {% endif %}
{%- endmacro -%}
