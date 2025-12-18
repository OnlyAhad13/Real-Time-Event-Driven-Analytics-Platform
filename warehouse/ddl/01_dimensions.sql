-- ============================================
-- Real-Time Analytics Platform
-- Star Schema DDL - Dimension Tables
-- ============================================
-- Compatible with: PostgreSQL, Snowflake, BigQuery (with minor syntax adjustments)

-- ============================================
-- DIMENSION: dim_time
-- ============================================
-- Pre-populated time dimension for date/time analysis
-- Typically populated with a date range (e.g., 2020-2030)

DROP TABLE IF EXISTS dim_time CASCADE;

CREATE TABLE dim_time (
    time_key            SERIAL PRIMARY KEY,
    full_timestamp      TIMESTAMP NOT NULL,
    date_actual         DATE NOT NULL,
    year                INTEGER NOT NULL,
    quarter             INTEGER NOT NULL,
    month               INTEGER NOT NULL,
    month_name          VARCHAR(20) NOT NULL,
    week_of_year        INTEGER NOT NULL,
    day_of_month        INTEGER NOT NULL,
    day_of_week         INTEGER NOT NULL,
    day_name            VARCHAR(20) NOT NULL,
    hour                INTEGER NOT NULL,
    minute              INTEGER NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_holiday          BOOLEAN DEFAULT FALSE,
    
    -- Unique constraint for timestamp lookup
    CONSTRAINT uq_dim_time_timestamp UNIQUE (full_timestamp)
);

-- Index for common lookups
CREATE INDEX idx_dim_time_date ON dim_time(date_actual);
CREATE INDEX idx_dim_time_year_month ON dim_time(year, month);

COMMENT ON TABLE dim_time IS 'Time dimension for temporal analysis';


-- ============================================
-- DIMENSION: dim_device
-- ============================================
-- Device dimension for device/platform analysis

DROP TABLE IF EXISTS dim_device CASCADE;

CREATE TABLE dim_device (
    device_key          SERIAL PRIMARY KEY,
    device_type         VARCHAR(50) NOT NULL,      -- mobile, desktop, tablet
    device_category     VARCHAR(50),               -- smartphone, laptop, etc.
    os_family           VARCHAR(100),              -- iOS, Android, Windows, macOS
    os_version          VARCHAR(50),
    browser_family      VARCHAR(100),              -- Chrome, Safari, Firefox
    browser_version     VARCHAR(50),
    is_mobile           BOOLEAN DEFAULT FALSE,
    is_tablet           BOOLEAN DEFAULT FALSE,
    is_desktop          BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Natural key constraint
    CONSTRAINT uq_dim_device_natural_key UNIQUE (device_type, os_family, browser_family)
);

-- Index for common lookups
CREATE INDEX idx_dim_device_type ON dim_device(device_type);
CREATE INDEX idx_dim_device_os ON dim_device(os_family);

COMMENT ON TABLE dim_device IS 'Device dimension for platform/device analysis';


-- ============================================
-- DIMENSION: dim_user (SCD Type-2)
-- ============================================
-- Slowly Changing Dimension Type 2
-- Tracks historical changes to user attributes (e.g., plan changes)

DROP TABLE IF EXISTS dim_user CASCADE;

CREATE TABLE dim_user (
    user_key            SERIAL PRIMARY KEY,        -- Surrogate key
    user_id             VARCHAR(100) NOT NULL,     -- Natural/Business key
    
    -- User attributes (trackable for SCD-2)
    email               VARCHAR(255),
    username            VARCHAR(100),
    plan_type           VARCHAR(50),               -- free, basic, premium, enterprise
    subscription_status VARCHAR(50),               -- active, cancelled, paused
    user_tier           VARCHAR(50),               -- bronze, silver, gold, platinum
    
    -- Geographic attributes
    country             VARCHAR(100),
    region              VARCHAR(100),
    city                VARCHAR(100),
    timezone            VARCHAR(50),
    
    -- Registration info
    signup_date         DATE,
    signup_source       VARCHAR(100),              -- organic, referral, paid_ad
    
    -- SCD Type-2 columns
    effective_start_date    TIMESTAMP NOT NULL,    -- When this version became active
    effective_end_date      TIMESTAMP,             -- When this version expired (NULL = current)
    is_current              BOOLEAN NOT NULL DEFAULT TRUE,
    version_number          INTEGER NOT NULL DEFAULT 1,
    
    -- Metadata
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for SCD-2 operations
CREATE INDEX idx_dim_user_user_id ON dim_user(user_id);
CREATE INDEX idx_dim_user_current ON dim_user(user_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_user_effective_dates ON dim_user(user_id, effective_start_date, effective_end_date);
CREATE INDEX idx_dim_user_plan ON dim_user(plan_type);

COMMENT ON TABLE dim_user IS 'User dimension with SCD Type-2 for tracking attribute changes over time';
COMMENT ON COLUMN dim_user.user_key IS 'Surrogate key - use this for joining to fact tables';
COMMENT ON COLUMN dim_user.user_id IS 'Natural key - the original user identifier';
COMMENT ON COLUMN dim_user.effective_start_date IS 'Start of validity period for this user version';
COMMENT ON COLUMN dim_user.effective_end_date IS 'End of validity period (NULL means currently active)';
COMMENT ON COLUMN dim_user.is_current IS 'Flag indicating if this is the current version of the user';
