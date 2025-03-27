{% macro remove_leading_numbers(column_name) %}
  REGEXP_REPLACE({{ column_name }}, r'^\d+\s*', '')
{% endmacro %}