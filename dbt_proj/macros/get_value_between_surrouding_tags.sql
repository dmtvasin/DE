{# Герасимов. Макрос принимает левый и правый разделитель в строке, возвращает значение между ними #}
{#  
    Пример вызова:
    select {{ get_value_between_surrouding_tags('tp_columnset', "'<nvarchar5>'", "'</nvarchar5>'") }} as card_type from {{ source('gp_public', 'ms_sp201_wss2023_alluserdata') }}
#}
{%- macro get_value_between_surrouding_tags(p_source_string, p_left_sep, p_right_sep) -%}
    {{
        split_part
        (
            string_text = split_part
                (
                    string_text = p_source_string,
                    delimiter_text = p_left_sep,
                    part_number = 2
                ),
            delimiter_text = p_right_sep,
            part_number = 1
        )
    }}
{%- endmacro -%}