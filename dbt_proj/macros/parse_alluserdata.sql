{#  Герасимов. Макрос парсит вложенные строки с кастомным разделителем
    Написан для разбора tp_columnset таблиц *alluserdata*, но может пригодиться и в другом месте.
    Принимает:  p_source_string - имя колонки или литерал, в примере = tp_columnset
                p_left_sep - левый разделитель в строке, в примере = "'<ntext21>'"
                p_right_sep - правый разделитель в строке, в примере = "'</ntext21>'"
                p_inner_sep, - внутренний разделитель в полученной строкев примере = "';#'"
                p_inner_part_number, в примере = 3
                ,   возвращает значение между ними #}
{#  
    Пример вызова:
    select {{ parse_alluserdata('tp_columnset', "'<ntext21>'", "'</ntext21>'", "';#'", 3) }} as contract_resbonsible from {{ source('gp_public', 'ms_sp201_wss2023_alluserdata') }}
#}
{%- macro parse_alluserdata(p_source_string, p_left_sep, p_right_sep, p_inner_sep, p_inner_part_number) -%}
    {{
        split_part
        (
            string_text = get_value_between_surrouding_tags
                (
                    p_source_string,
                    p_left_sep,
                    p_right_sep
                ),
            delimiter_text = p_inner_sep,
            part_number = p_inner_part_number
        )
    }}
{%- endmacro -%}

  {{
      split_part
      (
          get_value_between_surrouding_tags('tp_columnset', "'<ntext21>'", "'</ntext21>'")
          ,"';#'"
          ,3
      )
  }} as contract_resbonsible,