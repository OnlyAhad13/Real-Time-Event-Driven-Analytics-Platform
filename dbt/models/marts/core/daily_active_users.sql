{{
  config(
    materialized='table',
    tags=['core', 'daily']
  )
}}

/*
  Daily Active Users (DAU)
  ========================
  Count of unique users who had at least one event per day.
  
  Grain: One row per date
*/

WITH daily_events AS (
    SELECT
        DATE(event_timestamp) AS activity_date,
        user_key,
        COUNT(*) AS event_count
    FROM {{ source('warehouse', 'fact_events') }}
    WHERE event_timestamp IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    activity_date,
    COUNT(DISTINCT user_key) AS daily_active_users,
    SUM(event_count) AS total_events,
    ROUND(SUM(event_count)::DECIMAL / NULLIF(COUNT(DISTINCT user_key), 0), 2) AS avg_events_per_user,
    CURRENT_TIMESTAMP AS calculated_at
FROM daily_events
GROUP BY activity_date
ORDER BY activity_date DESC
