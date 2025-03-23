{% macro remove_dot_zero(column_name) %}
    CASE
        WHEN {{ column_name }} IS NOT NULL THEN REGEXP_REPLACE(CAST({{ column_name }} AS STRING), r'\.0$', '')
        ELSE {{ column_name }}
    END
{% endmacro %}