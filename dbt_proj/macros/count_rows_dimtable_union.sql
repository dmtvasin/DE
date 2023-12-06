--Макрос считающий кол-во уникальных строк в таблице по Id c Union All--

{% macro count_rows_dimtable_union(p_source_table_name, p_id_column_name) %}
SELECT
{{ n_days_diff(-1) }} as Date,
'{{ p_source_table_name }}' as Tabl,
count(distinct {{ p_id_column_name }}) as DistinctCount
FROM {{ ref(p_source_table_name ) }} source_table

Union All    
{% endmacro %}