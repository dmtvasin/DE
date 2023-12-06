{# Начало текущего месяца #}

{% macro BeginCurrentMonth(date_from) %}
    cast(date_trunc('MM', {{ date_from }} ) as date)
{% endmacro %}