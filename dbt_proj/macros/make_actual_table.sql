{# Герасимов. Макрос формирует актуальный срез из поданной на вход таблицы отбором версий с максимальной датой внутри одного id #}
{# - p_source_name - имя source из sources.yml.
    Если передать 'null', макрос вместо source - будет использовать ref().
    Если передать реально существующее имя source - будет использовать source().
    Это позволяет строить a_ из s_ данным макросом #}
{# - p_source_table_name - имя таблицы-источника из sources.yml ИЛИ имя модели ДБТ #}
{# - p_id_column_name - имя PK таблицы, из которой строим этим макросом модель #}
{# - p_date_distinct_column_name - имя watermark-колонки таблицы, из которой строим этим макросом модель #}
{%- macro make_actual_table(p_source_name, p_source_table_name, p_id_column_name, p_date_distinct_column_name) -%}
SELECT
    source_table.*
FROM 
    {{ source_or_ref(p_source_name, p_source_table_name) }} as source_table {# используем source или ref в зависимости от значения p_source_name #}
INNER JOIN
    (
    SELECT
        {{ p_id_column_name }},
        MAX({{ p_date_distinct_column_name }}) AS max_date
    FROM
        {{ source_or_ref(p_source_name, p_source_table_name) }}
    GROUP BY
        {{ p_id_column_name }}
    ) AS max_table
    ON source_table.{{ p_id_column_name }} = max_table.{{ p_id_column_name }}
    AND source_table.{{ p_date_distinct_column_name }} = max_table.max_date
Where 1 = 1
{%- if is_incremental() %}
    and source_table.{{ p_date_distinct_column_name }} > cast( {{ n_days_diff(-7) }} as {{ type_timestamp() }} )
{%- endif -%}
{%- endmacro -%}