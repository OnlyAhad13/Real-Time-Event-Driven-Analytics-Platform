-- ============================================
-- Real-Time Analytics Platform
-- Seed Data: dim_time population
-- ============================================
-- Generates time dimension data for 2020-2030

-- Populate dim_time with hourly granularity for a date range
-- This generates ~87,600 rows per year (24 hours * 365 days)

INSERT INTO dim_time (
    full_timestamp,
    date_actual,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_month,
    day_of_week,
    day_name,
    hour,
    minute,
    is_weekend
)
SELECT
    ts AS full_timestamp,
    ts::DATE AS date_actual,
    EXTRACT(YEAR FROM ts)::INTEGER AS year,
    EXTRACT(QUARTER FROM ts)::INTEGER AS quarter,
    EXTRACT(MONTH FROM ts)::INTEGER AS month,
    TO_CHAR(ts, 'Month') AS month_name,
    EXTRACT(WEEK FROM ts)::INTEGER AS week_of_year,
    EXTRACT(DAY FROM ts)::INTEGER AS day_of_month,
    EXTRACT(ISODOW FROM ts)::INTEGER AS day_of_week,
    TO_CHAR(ts, 'Day') AS day_name,
    EXTRACT(HOUR FROM ts)::INTEGER AS hour,
    0 AS minute,  -- Hourly granularity
    CASE WHEN EXTRACT(ISODOW FROM ts) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
    '2020-01-01 00:00:00'::TIMESTAMP,
    '2030-12-31 23:00:00'::TIMESTAMP,
    INTERVAL '1 hour'
) AS ts
ON CONFLICT (full_timestamp) DO NOTHING;

-- ============================================
-- Seed Data: dim_device (common device types)
-- ============================================

INSERT INTO dim_device (device_type, device_category, os_family, browser_family, is_mobile, is_tablet, is_desktop)
VALUES
    -- Mobile devices
    ('mobile', 'smartphone', 'iOS', 'Safari', TRUE, FALSE, FALSE),
    ('mobile', 'smartphone', 'iOS', 'Chrome', TRUE, FALSE, FALSE),
    ('mobile', 'smartphone', 'Android', 'Chrome', TRUE, FALSE, FALSE),
    ('mobile', 'smartphone', 'Android', 'Firefox', TRUE, FALSE, FALSE),
    ('mobile', 'smartphone', 'Android', 'Samsung Browser', TRUE, FALSE, FALSE),
    
    -- Tablets
    ('tablet', 'tablet', 'iOS', 'Safari', FALSE, TRUE, FALSE),
    ('tablet', 'tablet', 'Android', 'Chrome', FALSE, TRUE, FALSE),
    
    -- Desktop devices
    ('desktop', 'laptop', 'macOS', 'Safari', FALSE, FALSE, TRUE),
    ('desktop', 'laptop', 'macOS', 'Chrome', FALSE, FALSE, TRUE),
    ('desktop', 'laptop', 'macOS', 'Firefox', FALSE, FALSE, TRUE),
    ('desktop', 'laptop', 'Windows', 'Chrome', FALSE, FALSE, TRUE),
    ('desktop', 'laptop', 'Windows', 'Firefox', FALSE, FALSE, TRUE),
    ('desktop', 'laptop', 'Windows', 'Edge', FALSE, FALSE, TRUE),
    ('desktop', 'laptop', 'Linux', 'Chrome', FALSE, FALSE, TRUE),
    ('desktop', 'laptop', 'Linux', 'Firefox', FALSE, FALSE, TRUE),
    
    -- Unknown/Other
    ('unknown', 'unknown', 'Unknown', 'Unknown', FALSE, FALSE, FALSE)
ON CONFLICT (device_type, os_family, browser_family) DO NOTHING;


-- ============================================
-- Helper Views
-- ============================================

-- View for current users only (filters to is_current = TRUE)
CREATE OR REPLACE VIEW v_dim_user_current AS
SELECT *
FROM dim_user
WHERE is_current = TRUE;

COMMENT ON VIEW v_dim_user_current IS 'Convenience view showing only current user dimension records';


-- View for event analysis with all dimensions joined
CREATE OR REPLACE VIEW v_fact_events_denormalized AS
SELECT
    f.event_key,
    f.event_id,
    f.event_type,
    f.event_timestamp,
    
    -- User dimension
    u.user_id,
    u.email,
    u.plan_type,
    u.subscription_status,
    u.country AS user_country,
    
    -- Time dimension
    t.date_actual,
    t.year,
    t.month,
    t.month_name,
    t.day_name,
    t.hour,
    t.is_weekend,
    
    -- Device dimension
    d.device_type,
    d.os_family,
    d.browser_family,
    d.is_mobile,
    
    -- Measures
    f.purchase_amount,
    f.purchase_quantity,
    f.is_conversion,
    f.is_error
    
FROM fact_events f
LEFT JOIN dim_user u ON f.user_key = u.user_key
LEFT JOIN dim_time t ON f.time_key = t.time_key
LEFT JOIN dim_device d ON f.device_key = d.device_key;

COMMENT ON VIEW v_fact_events_denormalized IS 'Pre-joined view of fact_events with all dimensions for easy querying';
