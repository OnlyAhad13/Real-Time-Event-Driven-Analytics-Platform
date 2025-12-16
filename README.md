# Real-Time Event-Driven Analytics Platform

> Production-Grade Data Engineering Platform demonstrating Stream Processing, Data Warehousing, and Analytics Engineering

## 🏗️ Architecture

```
Event Producers (Python)
        ↓
Kafka (Streaming Ingestion)
        ↓
Spark Structured Streaming
        ↓
Data Lake (MinIO/S3) [Bronze → Silver → Gold]
        ↓
Data Warehouse (PostgreSQL/BigQuery/Snowflake)
        ↓
dbt Transformations
        ↓
Analytics / BI
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Make (optional, but recommended)
- 8GB+ RAM recommended

### Setup

```bash
# 1. Initialize the project
make init

# 2. Edit credentials in .env
nano .env

# 3. Start all services
make up

# 4. Check service status
make ps
```

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8090 | - |
| Spark Master | http://localhost:8080 | - |
| Spark Worker | http://localhost:8081 | - |
| MinIO Console | http://localhost:9001 | See `.env` |
| Jupyter Lab | http://localhost:8888 | Token in `.env` |
| PostgreSQL | localhost:5432 | See `.env` |

## 📁 Project Structure

```
.
├── docker-compose.yml      # Infrastructure definition
├── .env.template           # Environment variables template
├── Makefile               # Convenience commands
├── scripts/
│   ├── create-buckets.sh  # MinIO initialization
│   └── init-postgres.sql  # PostgreSQL schema
├── spark/
│   ├── apps/              # Spark applications
│   ├── data/              # Spark data (local)
│   └── jars/              # Additional JARs
├── notebooks/             # Jupyter notebooks
├── data/                  # Local data storage
└── logs/                  # Application logs
```

## 🛠️ Useful Commands

```bash
# View all available commands
make help

# Start/stop services
make up
make down
make restart

# View logs
make logs
make logs-kafka
make logs-spark

# Create Kafka topics
make kafka-create-topics

# Access shells
make postgres-shell
make pyspark-shell

# Clean up everything
make clean
```

## 📦 Services

### Kafka & Zookeeper
- **Confluent Platform 7.5.0**
- 3 partitions per topic (configurable)
- At-least-once delivery semantics

### Spark Cluster
- **Bitnami Spark 3.5.0**
- Master + Worker configuration
- Connected to Jupyter for development

### MinIO (S3-Compatible)
- Data Lake storage
- Buckets: `raw-events`, `bronze`, `silver`, `gold`

### PostgreSQL
- Data Warehouse with Star Schema
- Pre-configured dimensions and fact tables

### Jupyter Lab
- PySpark notebook environment
- Pre-installed: kafka-python, boto3, delta-spark

## 📋 Next Modules

- [ ] Event Generation (Python producers)
- [ ] Kafka Topics & Schemas
- [ ] Spark Structured Streaming
- [ ] Batch Processing
- [ ] dbt Transformations
- [ ] Airflow Orchestration
- [ ] Data Quality & Observability

---

**Built for MAANG-level Data Engineering Portfolio**
>>>>>>> ec6df79 (Added configurations)
