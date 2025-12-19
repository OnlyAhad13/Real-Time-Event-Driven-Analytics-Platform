{{
  config(
    materialized='table',
    tags=['core', 'revenue']
  )
}}

/*
  Revenue Per User
  ================
  Total purchase value aggregated per user.
  
  Grain: One row per user
*/

WITH user_purchases AS (
    SELECT
        f.user_key,
        u.user_id,
        u.email,
        u.plan_type,
        u.subscription_status,
        COUNT(*) AS total_purchases,
        SUM(f.purchase_amount) AS total_revenue,
        SUM(f.purchase_quantity) AS total_items,
        MIN(f.event_timestamp) AS first_purchase_at,
        MAX(f.event_timestamp) AS last_purchase_at
    FROM {{ source('warehouse', 'fact_events') }} f
    LEFT JOIN {{ source('warehouse', 'dim_user') }} u
        ON f.user_key = u.user_key
        AND u.is_current = TRUE
    WHERE f.event_type = 'purchase'
        AND f.purchase_amount > 0
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    user_key,
    user_id,
    email,
    plan_type,
    subscription_status,
    total_purchases,
    total_revenue,
    total_items,
    ROUND(total_revenue / NULLIF(total_purchases, 0), 2) AS avg_order_value,
    ROUND(total_items::DECIMAL / NULLIF(total_purchases, 0), 2) AS avg_items_per_order,
    first_purchase_at,
    last_purchase_at,
    (last_purchase_at::DATE - first_purchase_at::DATE) AS customer_lifespan_days,
    CURRENT_TIMESTAMP AS calculated_at
FROM user_purchases
ORDER BY total_revenue DESC
