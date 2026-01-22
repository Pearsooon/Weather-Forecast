{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="CURRENT_DATE()"
    )}}
),

with_date_attributes AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY date_day) AS date_id,
        date_day AS date,
        
        -- Date components
        DAY(date_day) AS day,
        MONTH(date_day) AS month,
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        DAYOFWEEK(date_day) AS day_of_week,
        WEEKOFYEAR(date_day) AS week_of_year,
        
        -- Day name
        CASE DAYOFWEEK(date_day)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_name,
        
        -- Month name
        CASE MONTH(date_day)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,
        
        -- Weekend flag
        CASE 
            WHEN DAYOFWEEK(date_day) IN (0, 6) THEN 1 
            ELSE 0 
        END AS is_weekend,
        
        -- Season (Vietnam context)
        CASE 
            WHEN MONTH(date_day) IN (11, 12, 1, 2, 3, 4) THEN 'Dry Season'
            ELSE 'Rainy Season'
        END AS season,
        
        CURRENT_TIMESTAMP() AS created_at
        
    FROM date_spine
)

SELECT * FROM with_date_attributes