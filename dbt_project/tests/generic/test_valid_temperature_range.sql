{% test valid_temperature_range(model, column_name) %}

-- Test that temperature values are within valid range (-50 to 60°C)
SELECT
    {{ column_name }},
    COUNT(*) as invalid_count
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
    AND ({{ column_name }} < -50 OR {{ column_name }} > 60)
GROUP BY {{ column_name }}
HAVING COUNT(*) > 0

{% endtest %}