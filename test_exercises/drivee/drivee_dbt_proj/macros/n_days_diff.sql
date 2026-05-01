--Макрос отображающий текущую дату c вычетом/сложением указанного кол-во дней--

{% macro n_days_diff(count_day) %}
    {{ dbt_date.today() }} + {{ count_day }}
{% endmacro %}