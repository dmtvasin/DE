{# Возвращает дату на 365 дней назад #}

{% macro PreviousYear(date_from) %}
    cast(date_trunc('DD', {{ date_from }} ) as date) -365
{% endmacro %}