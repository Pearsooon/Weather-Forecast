WITH quality_stats AS (
    SELECT
        location_name,
        AVG(data_quality_score) as avg_quality_score,
        MIN(data_quality_score) as min_quality_score,
        COUNT(*) as total_records
    FROM {{ ref('int_weather_quality_checked') }}
    GROUP BY location_name
)

SELECT
    location_name,
    avg_quality_score,
    min_quality_score,
    total_records
FROM quality_stats
WHERE avg_quality_score < 80