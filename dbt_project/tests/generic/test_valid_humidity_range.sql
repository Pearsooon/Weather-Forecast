{% test valid_humidity_range(model, column_name) %}

-- Test that humidity values are within valid range (0-100%)
SELECT
    {{ column_name }},
    COUNT(*) as invalid_count
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
    AND ({{ column_name }} < 0 OR {{ column_name }} > 100)
GROUP BY {{ column_name }}
HAVING COUNT(*) > 0

{% endtest %}