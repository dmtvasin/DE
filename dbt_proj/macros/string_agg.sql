--Макрос STRING_AGG--

{% macro string_agg(name_column, delimiter) %}
    STRING_AGG({{ name_column }}, '{{ delimiter }}')
{% endmacro %}

{# Для брикса

{% macro string_agg(name_column, delimiter) %}
    array_join(array_sort(collect_set({{ name_column }}, '{{ delimiter }}')))
{% endmacro %}
#}