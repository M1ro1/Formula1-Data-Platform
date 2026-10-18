{% macro lap_time_to_seconds(column) -%}
(
    case
        when {{ column }} is null then null
        when {{ column }} like '%:%' then
            try_to_number(split_part({{ column }}, ':', 1)) * 60
            + try_to_decimal(split_part({{ column }}, ':', 2), 10, 3)
        else try_to_decimal({{ column }}, 10, 3)
    end
)
{%- endmacro %}