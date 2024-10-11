{% macro convert_timezone_from_utc(column_name) %}
    convert_timezone('UTC','America/New_York', {{ column_name }}::timestamp_ntz)
{% endmacro %} 