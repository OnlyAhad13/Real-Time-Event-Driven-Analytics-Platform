# Real-Time Event-Driven Analytics Platform

<p align="center">
  <img src="https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white" alt="Kafka"/>
  <img src="https://img.shields.io/badge/Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white" alt="Spark"/>
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL"/>
  <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt"/>
  <img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white" alt="Airflow"/>
</p>

A production-grade, end-to-end real-time analytics platform implementing the **Medallion Architecture** (Bronze → Silver → Gold) with event-driven streaming, batch processing, and data quality observability.

---

## 📋 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Quick Start](#-quick-start)
- [Components](#-components)
- [Data Pipeline](#-data-pipeline)
- [Design Decisions](#-design-decisions)
- [Usage Guide](#-usage-guide)
- [Monitoring & Observability](#-monitoring--observability)
- [Development](#-development)
- [Troubleshooting](#-troubleshooting)

---

## 🏗 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         REAL-TIME ANALYTICS PLATFORM                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────────────┐ │
│  │   Producer   │────▶│    Kafka     │────▶│   Spark Structured Streaming │ │
│  │   (Python)   │     │  (events_raw)│     │                              │ │
│  └──────────────┘     └──────────────┘     │  ┌────────┐    ┌────────┐    │ │
│                                            │  │ Bronze │───▶│ Silver │    │ │
│                                            │  └────────┘    └────────┘    │ │
│                                            └──────────────────────────────┘ │
│                                                                             │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                           DATA LAKE (MinIO/S3)                       │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐   │   │
│  │  │ bronze/events/  │  │ silver/events/  │  │ dead-letter-queue/  │   │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────────┘   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    SPARK BATCH LOADER (Daily)                        │   │
│  │  • SCD Type-2 Updates (dim_user)                                     │   │
│  │  • Incremental Fact Loading (fact_events)                            │   │
│  │  • Dimension Key Lookups                                             │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                      DATA WAREHOUSE (PostgreSQL)                     │   │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌─────────────────┐    │   │
│  │  │ dim_time  │  │dim_device │  │ dim_user  │  │  fact_events    │    │   │
│  │  │ (96K rows)│  │ (16 rows) │  │ (SCD-2)   │  │  (Central Fact) │    │   │
│  │  └───────────┘  └───────────┘  └───────────┘  └─────────────────┘    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                          dbt TRANSFORMATIONS                         │   │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐    │   │
│  │  │daily_active_users│  │ revenue_per_user │  │   churn_risk     │    │   │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         ORCHESTRATION (Airflow)                      │   │
│  │  [spark_batch_job] ──▶ [dbt_run] ──▶ [dbt_test] ──▶ [data_quality]   │   │
│  │                        Daily @ 2 AM UTC                              │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Ingestion** | Kafka (Confluent) | Event streaming, message durability |
| **Processing** | Spark Structured Streaming | Real-time and batch transformations |
| **Storage** | MinIO (S3-compatible) | Data lake with Parquet files |
| **Warehouse** | PostgreSQL | Star schema analytics warehouse |
| **Transformation** | dbt | SQL-based data modeling |
| **Orchestration** | Apache Airflow | DAG scheduling and monitoring |
| **Observability** | Custom Python | Data quality checks and alerting |

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose v2.0+
- Python 3.11+ (for local development)
- 8GB RAM minimum (16GB recommended)
- 20GB disk space

### 1. Clone and Setup

```bash
git clone https://github.com/your-org/real-time-analytics.git
cd real-time-analytics

# Copy environment template
cp .env.example .env
```

### 2. Start Infrastructure

```bash
# Start all services (Kafka, Spark, MinIO, PostgreSQL, Airflow)
make up

# Verify all services are healthy
make status
```

**Services will be available at:**

| Service | URL | Credentials |
|---------|-----|-------------|
| Spark Master UI | http://localhost:8080 | - |
| MinIO Console | http://localhost:9001 | minio_admin / minio_password_change_me |
| PostgreSQL | localhost:5433 | analytics_user / analytics_password_change_me |
| Airflow | http://localhost:8081 | airflow / airflow |
| Kafka | localhost:29093 | - |

### 3. Initialize Kafka Topics

```bash
# Create required topics
python scripts/init_kafka.py
```

### 4. Initialize Warehouse Schema

```bash
# Create star schema tables
make postgres-shell
\i /path/to/warehouse/ddl/01_dimensions.sql
\i /path/to/warehouse/ddl/02_facts.sql
\i /path/to/warehouse/ddl/03_seed_data.sql
\q

# Or using the concatenated command:
cat warehouse/ddl/*.sql | docker compose exec -T postgres psql -U analytics_user -d analytics_warehouse
```

### 5. Generate Events

```bash
# Generate 1000 events at 50 events/second
python -m producers.producer --total-events 1000 --events-per-second 50
```

### 6. Start Streaming Jobs

```bash
# Bronze layer (Kafka → MinIO)
make submit-bronze

# Silver layer (Bronze → Sessionized)
make submit-silver

# Or both together
make submit-all
```

### 7. Run Batch ETL

```bash
# Load yesterday's data into warehouse
make submit-batch

# Or for a specific date
make submit-batch DATE=2024-12-17
```

### 8. Run dbt Transformations

```bash
cd dbt
dbt run
dbt test
```

---

## 📦 Components

### Event Producer (`producers/`)

High-throughput Python producer generating realistic e-commerce events:

```bash
python -m producers.producer \
    --total-events 10000 \
    --events-per-second 100 \
    --bad-data-rate 0.01
```

**Event Types:**
- `user_signup` - New user registration
- `page_view` - Page navigation events
- `purchase` - Completed transactions
- `payment_failed` - Failed payment attempts

**Features:**
- Schema versioning (v1)
- Idempotency keys for exactly-once semantics
- Configurable bad data injection (1% default)
- Retry logic with exponential backoff

### Spark Streaming Job (`spark/apps/streaming_job.py`)

Dual-layer streaming processor:

| Mode | Input | Output | Features |
|------|-------|--------|----------|
| `bronze` | Kafka | `s3a://bronze/events/` | Raw ingestion, date partitioning |
| `silver` | Bronze Parquet | `s3a://silver/events/` | Validation, dedup, sessionization |

```bash
# Run specific mode
make submit-bronze
make submit-silver
make submit-all  # Both layers
```

### Batch Loader (`spark/apps/batch_loader.py`)

Daily batch job for warehouse population:

- **SCD Type-2** for `dim_user` (tracks plan changes)
- **Incremental upserts** for `fact_events`
- **Dimension key lookups** (user, time, device)

### dbt Models (`dbt/models/`)

| Model | Description | Materialization |
|-------|-------------|-----------------|
| `daily_active_users` | DAU with event counts | Table |
| `revenue_per_user` | LTV, AOV, purchase stats | Table |
| `churn_risk` | Users inactive 7+ days | Table |

### Data Quality (`scripts/data_quality_checks.py`)

Production-grade observability:

```bash
python scripts/data_quality_checks.py \
    --date 2024-12-17 \
    --slack-webhook https://hooks.slack.com/...
```

| Check | Threshold | Action on Failure |
|-------|-----------|-------------------|
| Freshness | < 24 hours old | Alert |
| Volume | ±20% of 7-day avg | Alert |
| Nulls | 0% on critical columns | Alert |

---

## 🔄 Data Pipeline

### Streaming Pipeline (Real-Time)

```
Producer → Kafka (events_raw) → Spark Streaming
                                      │
                           ┌──────────┴──────────┐
                           ▼                     ▼
                     Bronze Layer           DLQ (Invalid)
                    (Raw Parquet)
                           │
                           ▼
                     Silver Layer
                   (Sessionized Parquet)
```

### Batch Pipeline (Daily @ 2 AM UTC)

```
Silver Layer → Spark Batch Loader → Data Warehouse
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
    dim_user     dim_time    fact_events
    (SCD-2)      (Lookup)    (Upsert)
                                  │
                                  ▼
                           dbt Transformations
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
              daily_active   revenue_per    churn_risk
                 _users         _user
```

---

## 🎯 Design Decisions

### 1. Partitioning Strategy

| Layer | Partition Keys | Rationale |
|-------|----------------|-----------|
| **Bronze** | `date`, `event_type` | Enables efficient date-range scans and event filtering |
| **Silver** | `date` | Sessionized data grouped by window start date |
| **Warehouse** | None (indexed) | OLAP queries benefit from star schema joins |

**Why these choices:**
- **Bronze `date` partition**: Critical for time-based backfills and retention policies
- **Bronze `event_type` partition**: Allows processing specific event types without full scans
- **Warehouse indexes over partitions**: PostgreSQL's query planner optimizes joins better with proper indexes on star schema

### 2. Stream vs. Batch Tradeoffs

| Concern | Streaming Approach | Batch Approach | Our Choice |
|---------|-------------------|----------------|------------|
| **Latency** | Sub-second | Hours | Streaming for Bronze/Silver |
| **Exactly-once** | Complex (checkpointing) | Simple (idempotent) | Hybrid: streaming with checkpoints, batch with upserts |
| **Cost** | Higher (always-on) | Lower (scheduled) | Streaming for ingestion, batch for warehouse |
| **Complexity** | Higher | Lower | Accept complexity for real-time Bronze/Silver |

**Our Lambda-lite Architecture:**
1. **Streaming Layer**: Bronze and Silver for low-latency data availability
2. **Batch Layer**: Daily warehouse loads with SCD-2 and aggregations
3. **No Serving Layer Duplication**: Warehouse is the single source of truth for analytics

### 3. SCD Type-2 Implementation

Chose Type-2 over Type-1 for `dim_user` because:
- **Audit trail**: Track when users changed plans
- **Historical accuracy**: Reports reflect user status at event time
- **Point-in-time queries**: Join fact to dimension as of event date

```sql
-- Example: Revenue by plan at time of purchase
SELECT u.plan_type, SUM(f.purchase_amount)
FROM fact_events f
JOIN dim_user u ON f.user_key = u.user_key
WHERE f.event_timestamp BETWEEN u.effective_start_date 
                           AND COALESCE(u.effective_end_date, '9999-12-31')
GROUP BY u.plan_type;
```

### 4. Idempotency Strategy

| Component | Mechanism |
|-----------|-----------|
| Producer | `idempotency_key` in event payload |
| Bronze | Append-only (duplicates preserved for audit) |
| Silver | `dropDuplicates(["idempotency_key"])` with watermark |
| Warehouse | `INSERT ON CONFLICT (idempotency_key) DO NOTHING` |

### 5. Schema Evolution Approach

- **Event Schema**: Versioned (`schema_version: "v1"`)
- **Bronze**: Stores raw JSON for maximum flexibility
- **Silver**: Applies schema validation, routes failures to DLQ
- **Warehouse**: Strict schema with migrations managed separately

---

## 📊 Monitoring & Observability

### Data Quality Alerts

```bash
# Daily quality check (add to cron or Airflow)
python scripts/data_quality_checks.py \
    --date $(date -d "yesterday" +%Y-%m-%d) \
    --slack-webhook $SLACK_WEBHOOK \
    --output-json /var/log/dq/report_$(date +%Y%m%d).json
```

### Key Metrics to Monitor

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| Kafka Lag | Kafka Consumer Groups | > 10,000 messages |
| Streaming Batch Duration | Spark UI | > 30 seconds |
| Warehouse Load Time | Airflow | > 1 hour |
| dbt Test Failures | dbt Cloud / Airflow | Any failure |
| DLQ Volume | MinIO | > 1% of total |

### Spark UI Access

```bash
# Spark Master UI
open http://localhost:8080

# Streaming Query Progress
open http://localhost:4040/streaming/  # (when job running)
```

---

## 🛠 Development

### Local Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r producers/requirements.txt
pip install -r scripts/requirements.txt
pip install dbt-core dbt-postgres

# Run tests
pytest tests/ -v
```

### Project Structure

```
real-time-analytics/
├── airflow/
│   └── dags/
│       └── analytics_pipeline.py    # Airflow DAG
├── dbt/
│   ├── dbt_project.yml              # dbt configuration
│   ├── profiles.yml                 # Connection profiles
│   └── models/
│       ├── schema.yml               # Tests and docs
│       └── marts/core/              # dbt models
├── producers/
│   ├── producer.py                  # Event producer
│   ├── generator.py                 # Event generation logic
│   ├── schemas.py                   # Event schemas
│   └── config.py                    # Configuration
├── scripts/
│   ├── init_kafka.py                # Topic initialization
│   ├── data_quality_checks.py       # DQ observability
│   └── create-buckets.sh            # MinIO setup
├── spark/
│   └── apps/
│       ├── streaming_job.py         # Bronze/Silver streaming
│       └── batch_loader.py          # Warehouse ETL
├── warehouse/
│   └── ddl/
│       ├── 01_dimensions.sql        # Dimension tables
│       ├── 02_facts.sql             # Fact table
│       └── 03_seed_data.sql         # Initial data
├── docker-compose.yml               # Infrastructure
├── Dockerfile                       # Python container
├── Makefile                         # Common commands
└── README.md                        # This file
```

### Useful Commands

```bash
make up              # Start all services
make down            # Stop all services
make status          # Check service health
make logs-spark      # View Spark logs
make logs-kafka      # View Kafka logs
make submit-bronze   # Run Bronze streaming job
make submit-silver   # Run Silver streaming job
make submit-batch    # Run batch warehouse load
make postgres-shell  # PostgreSQL CLI
make kafka-topics    # List Kafka topics
```

---

## 🔧 Troubleshooting

### Kafka Connection Issues

```bash
# Verify Kafka is running
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check ports
nc -zv localhost 29093
```

### Spark Job Failures

```bash
# Check Spark logs
docker compose logs spark-master -f

# Access Spark UI
open http://localhost:8080
```

### MinIO Access Issues

```bash
# Verify buckets exist
docker compose exec minio mc ls myminio/

# Check MinIO logs
docker compose logs minio
```

### dbt Connection Issues

```bash
# Test connection
cd dbt && dbt debug

# Check profiles.yml has correct port (5433 for Docker)
```

---

## 📄 License

MIT License - See [LICENSE](LICENSE) for details.

---

## 👥 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

<p align="center">
  Built with ❤️ by Syed Abdul Ahad
</p>
