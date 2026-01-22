{% test non_negative(model, column_name) %}

-- Test that values are non-negative
SELECT
    {{ column_name }},
    COUNT(*) as invalid_count
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
    AND {{ column_name }} < 0
GROUP BY {{ column_name }}
HAVING COUNT(*) > 0

{% endtest %}