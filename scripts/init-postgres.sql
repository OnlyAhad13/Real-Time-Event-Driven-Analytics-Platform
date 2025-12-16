-- ============================================
-- PostgreSQL Initialization Script
-- Analytics Data Warehouse Schema
-- ============================================

-- Create schemas for data warehouse layers
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS metadata;

-- ============================================
-- METADATA TABLES
-- ============================================

-- Schema registry for event versioning
CREATE TABLE IF NOT EXISTS metadata.schema_registry (
    schema_id SERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    schema_version INTEGER NOT NULL,
    schema_definition JSONB NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(event_type, schema_version)
);

-- Job execution tracking
CREATE TABLE IF NOT EXISTS metadata.job_runs (
    run_id SERIAL PRIMARY KEY,
    job_name VARCHAR(200) NOT NULL,
    job_type VARCHAR(50) NOT NULL, -- 'streaming', 'batch', 'dbt'
    status VARCHAR(20) NOT NULL, -- 'running', 'success', 'failed'
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    records_processed BIGINT DEFAULT 0,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Data quality checks log
CREATE TABLE IF NOT EXISTS metadata.data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    check_name VARCHAR(200) NOT NULL,
    table_name VARCHAR(200) NOT NULL,
    check_type VARCHAR(50) NOT NULL, -- 'freshness', 'schema', 'volume', 'anomaly'
    status VARCHAR(20) NOT NULL, -- 'pass', 'fail', 'warning'
    check_value NUMERIC,
    threshold_value NUMERIC,
    details JSONB,
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- DIMENSION TABLES (Star Schema)
-- ============================================

-- Time dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_time (
    time_key SERIAL PRIMARY KEY,
    full_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    date_day DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    hour INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT false,
    UNIQUE(full_timestamp)
);

-- User dimension (SCD Type 2 ready)
CREATE TABLE IF NOT EXISTS warehouse.dim_user (
    user_key SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    username VARCHAR(100),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(100),
    city VARCHAR(100),
    signup_source VARCHAR(50),
    user_tier VARCHAR(20), -- 'free', 'premium', 'enterprise'
    -- SCD Type 2 columns
    effective_from TIMESTAMP WITH TIME ZONE NOT NULL,
    effective_to TIMESTAMP WITH TIME ZONE DEFAULT '9999-12-31 23:59:59+00',
    is_current BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Product/Page dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    price NUMERIC(10, 2),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Event type dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_event_type (
    event_type_key SERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL UNIQUE,
    event_category VARCHAR(50), -- 'user_action', 'transaction', 'system'
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- FACT TABLES
-- ============================================

-- Main events fact table (partitioned by date in production)
CREATE TABLE IF NOT EXISTS warehouse.fact_events (
    event_key BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL UNIQUE,
    event_type_key INTEGER REFERENCES warehouse.dim_event_type(event_type_key),
    user_key INTEGER REFERENCES warehouse.dim_user(user_key),
    time_key INTEGER REFERENCES warehouse.dim_time(time_key),
    product_key INTEGER REFERENCES warehouse.dim_product(product_key),
    -- Event attributes
    session_id VARCHAR(100),
    page_url VARCHAR(500),
    referrer_url VARCHAR(500),
    device_type VARCHAR(50),
    browser VARCHAR(50),
    os VARCHAR(50),
    -- Metrics
    event_value NUMERIC(15, 2),
    quantity INTEGER,
    -- Metadata
    schema_version INTEGER,
    idempotency_key VARCHAR(100),
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Purchases fact table
CREATE TABLE IF NOT EXISTS warehouse.fact_purchases (
    purchase_key BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL UNIQUE,
    user_key INTEGER REFERENCES warehouse.dim_user(user_key),
    time_key INTEGER REFERENCES warehouse.dim_time(time_key),
    product_key INTEGER REFERENCES warehouse.dim_product(product_key),
    -- Transaction details
    transaction_id VARCHAR(100),
    payment_method VARCHAR(50),
    currency VARCHAR(10),
    amount NUMERIC(15, 2) NOT NULL,
    tax_amount NUMERIC(15, 2),
    discount_amount NUMERIC(15, 2),
    quantity INTEGER DEFAULT 1,
    status VARCHAR(20), -- 'completed', 'refunded', 'failed'
    -- Metadata
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

-- ============================================
-- ANALYTICS TABLES (dbt will manage these)
-- ============================================

-- Placeholder for dbt-managed tables
COMMENT ON SCHEMA analytics IS 'Analytics tables managed by dbt';

-- ============================================
-- INDEXES
-- ============================================

CREATE INDEX IF NOT EXISTS idx_fact_events_timestamp ON warehouse.fact_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_events_user ON warehouse.fact_events(user_key);
CREATE INDEX IF NOT EXISTS idx_fact_events_type ON warehouse.fact_events(event_type_key);
CREATE INDEX IF NOT EXISTS idx_fact_purchases_timestamp ON warehouse.fact_purchases(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_purchases_user ON warehouse.fact_purchases(user_key);
CREATE INDEX IF NOT EXISTS idx_dim_user_current ON warehouse.dim_user(user_id) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS idx_job_runs_status ON metadata.job_runs(status, start_time);

-- ============================================
-- SEED DATA
-- ============================================

-- Insert common event types
INSERT INTO warehouse.dim_event_type (event_type, event_category, description) VALUES
    ('user_signup', 'user_action', 'User registration event'),
    ('page_view', 'user_action', 'Page view event'),
    ('purchase', 'transaction', 'Purchase completion event'),
    ('payment_failed', 'transaction', 'Failed payment attempt'),
    ('add_to_cart', 'user_action', 'Product added to cart'),
    ('remove_from_cart', 'user_action', 'Product removed from cart'),
    ('search', 'user_action', 'Search query event'),
    ('logout', 'user_action', 'User logout event')
ON CONFLICT (event_type) DO NOTHING;

-- ============================================
-- GRANTS
-- ============================================

-- Grant permissions (adjust as needed for your setup)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO analytics_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO analytics_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO analytics_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO analytics_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata TO analytics_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA warehouse TO analytics_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA metadata TO analytics_user;

RAISE NOTICE 'PostgreSQL initialization complete!';
