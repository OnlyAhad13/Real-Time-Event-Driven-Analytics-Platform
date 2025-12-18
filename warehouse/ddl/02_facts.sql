-- ============================================
-- Real-Time Analytics Platform
-- Star Schema DDL - Fact Table
-- ============================================
-- Compatible with: PostgreSQL, Snowflake, BigQuery (with minor syntax adjustments)

-- ============================================
-- FACT TABLE: fact_events
-- ============================================
-- Central fact table for event analytics
-- Grain: One row per event

DROP TABLE IF EXISTS fact_events CASCADE;

CREATE TABLE fact_events (
    -- Surrogate key
    event_key           BIGSERIAL PRIMARY KEY,
    
    -- Natural keys (for deduplication)
    event_id            VARCHAR(100) NOT NULL,
    idempotency_key     VARCHAR(100) NOT NULL,
    
    -- Foreign keys to dimensions
    user_key            INTEGER REFERENCES dim_user(user_key),
    time_key            INTEGER REFERENCES dim_time(time_key),
    device_key          INTEGER REFERENCES dim_device(device_key),
    
    -- Degenerate dimensions (event-specific attributes)
    event_type          VARCHAR(50) NOT NULL,       -- user_signup, page_view, purchase, payment_failed
    schema_version      VARCHAR(20),
    
    -- Event details
    session_id          VARCHAR(100),
    ip_address          VARCHAR(45),                -- IPv6 compatible
    
    -- Geographic context (denormalized for performance)
    geo_country         VARCHAR(100),
    geo_region          VARCHAR(100),
    geo_city            VARCHAR(100),
    geo_latitude        DECIMAL(10, 8),
    geo_longitude       DECIMAL(11, 8),
    
    -- Measures / Metrics
    page_view_count     INTEGER DEFAULT 0,
    purchase_amount     DECIMAL(12, 2) DEFAULT 0,
    purchase_quantity   INTEGER DEFAULT 0,
    is_conversion       BOOLEAN DEFAULT FALSE,
    is_error            BOOLEAN DEFAULT FALSE,
    
    -- Payload storage (JSON for flexibility)
    payload_json        JSONB,
    
    -- Timestamps
    event_timestamp     TIMESTAMP NOT NULL,         -- When the event occurred
    kafka_timestamp     TIMESTAMP,                  -- When Kafka received the event
    ingestion_timestamp TIMESTAMP,                  -- When Bronze layer ingested the event
    warehouse_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Data quality
    data_quality_score  DECIMAL(3, 2),              -- 0.00 to 1.00
    
    -- Unique constraint for idempotency
    CONSTRAINT uq_fact_events_idempotency UNIQUE (idempotency_key)
);

-- ============================================
-- Indexes for fact_events
-- ============================================

-- Foreign key indexes (critical for joins)
CREATE INDEX idx_fact_events_user_key ON fact_events(user_key);
CREATE INDEX idx_fact_events_time_key ON fact_events(time_key);
CREATE INDEX idx_fact_events_device_key ON fact_events(device_key);

-- Common query patterns
CREATE INDEX idx_fact_events_event_type ON fact_events(event_type);
CREATE INDEX idx_fact_events_timestamp ON fact_events(event_timestamp);
CREATE INDEX idx_fact_events_user_time ON fact_events(user_key, event_timestamp);

-- Partial indexes for specific event types
CREATE INDEX idx_fact_events_purchases ON fact_events(user_key, purchase_amount) 
    WHERE event_type = 'purchase';
CREATE INDEX idx_fact_events_errors ON fact_events(event_timestamp) 
    WHERE is_error = TRUE;

-- ============================================
-- Comments
-- ============================================

COMMENT ON TABLE fact_events IS 'Central fact table for event analytics. Grain: one row per event.';
COMMENT ON COLUMN fact_events.event_key IS 'Surrogate primary key';
COMMENT ON COLUMN fact_events.idempotency_key IS 'Natural key for deduplication - ensures exactly-once semantics';
COMMENT ON COLUMN fact_events.user_key IS 'FK to dim_user - use point-in-time lookup for SCD-2';
COMMENT ON COLUMN fact_events.payload_json IS 'Full event payload stored as JSONB for ad-hoc analysis';
