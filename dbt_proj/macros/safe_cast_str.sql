{%- macro safe_cast_str(p_value, p_datatype) -%}
  cast(
        case {{ p_value }}
          when '' then null
          else {{ p_value }}
        end as {{ p_datatype }}
      )
{%- endmacro -%}