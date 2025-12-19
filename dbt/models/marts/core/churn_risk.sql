{{
  config(
    materialized='table',
    tags=['core', 'churn']
  )
}}

/*
  Churn Risk Analysis
  ===================
  Identify users who haven't logged in for 7+ days.
  
  Grain: One row per user at churn risk
*/

WITH user_last_activity AS (
    SELECT
        f.user_key,
        u.user_id,
        u.email,
        u.plan_type,
        u.subscription_status,
        u.signup_date,
        MAX(f.event_timestamp) AS last_activity_at,
        COUNT(*) AS total_events,
        COUNT(DISTINCT DATE(f.event_timestamp)) AS active_days
    FROM {{ source('warehouse', 'fact_events') }} f
    LEFT JOIN {{ source('warehouse', 'dim_user') }} u
        ON f.user_key = u.user_key
        AND u.is_current = TRUE
    GROUP BY 1, 2, 3, 4, 5, 6
),

churn_analysis AS (
    SELECT
        user_key,
        user_id,
        email,
        plan_type,
        subscription_status,
        signup_date,
        last_activity_at,
        total_events,
        active_days,
        CURRENT_DATE - last_activity_at::DATE AS days_since_last_activity,
        CASE
            WHEN CURRENT_DATE - last_activity_at::DATE >= {{ var('churn_lookback_days', 7) }}
            THEN TRUE
            ELSE FALSE
        END AS is_at_churn_risk,
        CASE
            WHEN CURRENT_DATE - last_activity_at::DATE >= 30 THEN 'high'
            WHEN CURRENT_DATE - last_activity_at::DATE >= 14 THEN 'medium'
            WHEN CURRENT_DATE - last_activity_at::DATE >= {{ var('churn_lookback_days', 7) }} THEN 'low'
            ELSE 'none'
        END AS churn_risk_level
    FROM user_last_activity
)

SELECT
    user_key,
    user_id,
    email,
    plan_type,
    subscription_status,
    signup_date,
    last_activity_at,
    total_events,
    active_days,
    days_since_last_activity,
    is_at_churn_risk,
    churn_risk_level,
    CURRENT_TIMESTAMP AS calculated_at
FROM churn_analysis
WHERE is_at_churn_risk = TRUE
ORDER BY days_since_last_activity DESC
