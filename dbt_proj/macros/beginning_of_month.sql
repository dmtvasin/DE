{% macro beginning_of_month(in_date) %}
    {{ dbt_utils.date_trunc('month', in_date) }}
{% endmacro %}