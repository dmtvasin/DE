{% macro one_year_before(date_from) %}
    ({{ dbt_utils.dateadd(datepart = 'year', interval = -1, from_date_or_timestamp =  date_from ) }}::date)
{% endmacro %}