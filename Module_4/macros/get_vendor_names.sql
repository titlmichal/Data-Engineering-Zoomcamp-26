{% macro get_vendor_names(vendor_id) -%}
case
    when {{ vendor_id }} = 1 then 'Creative tech'
    when {{ vendor_id }} = 2 then 'Verifone'
    when {{ vendor_id }} = 4 then 'Unknown'
end
{%- endmacro %}