# ============================================
# Real-Time Analytics Platform - Makefile
# ============================================

.PHONY: help build up down restart logs ps clean init setup-env

# Default target
help:
	@echo "============================================"
	@echo "Real-Time Analytics Platform - Commands"
	@echo "============================================"
	@echo ""
	@echo "Setup:"
	@echo "  make setup-env    - Create .env from template"
	@echo "  make init         - Initialize project (create dirs, .env)"
	@echo ""
	@echo "Docker:"
	@echo "  make build        - Build all containers"
	@echo "  make up           - Start all services"
	@echo "  make down         - Stop all services"
	@echo "  make restart      - Restart all services"
	@echo "  make logs         - View logs (all services)"
	@echo "  make ps           - Show running containers"
	@echo "  make clean        - Remove all containers and volumes"
	@echo ""
	@echo "Individual Services:"
	@echo "  make up-kafka     - Start Kafka stack only"
	@echo "  make up-spark     - Start Spark cluster only"
	@echo "  make up-storage   - Start MinIO + PostgreSQL only"
	@echo ""
	@echo "Access URLs:"
	@echo "  Kafka UI:       http://localhost:8090"
	@echo "  Spark Master:   http://localhost:8080"
	@echo "  Spark Worker:   http://localhost:8081"
	@echo "  MinIO Console:  http://localhost:9001"
	@echo "  Jupyter Lab:    http://localhost:8888"
	@echo "  PostgreSQL:     localhost:5432"
	@echo ""

# ==================== SETUP ====================

setup-env:
	@if [ ! -f .env ]; then \
		cp .env.template .env; \
		echo "Created .env from template. Please update credentials!"; \
	else \
		echo ".env already exists. Skipping."; \
	fi

init: setup-env
	@mkdir -p spark/apps spark/data spark/jars
	@mkdir -p notebooks/exploration notebooks/pipelines notebooks/analysis
	@mkdir -p data/lake data/warehouse
	@mkdir -p logs
	@chmod +x scripts/*.sh 2>/dev/null || true
	@echo "Project initialized successfully!"

# ==================== DOCKER COMMANDS ====================

build:
	docker compose build

up: init
	docker compose up -d
	@echo ""
	@echo "Services starting... Use 'make logs' to view logs"
	@echo "Use 'make ps' to check service status"

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down -v --remove-orphans
	@echo "All containers and volumes removed"

# ==================== INDIVIDUAL SERVICES ====================

up-kafka:
	docker compose up -d zookeeper kafka kafka-ui
	@echo "Kafka stack started"

up-spark:
	docker compose up -d spark-master spark-worker
	@echo "Spark cluster started"

up-storage:
	docker compose up -d minio minio-init postgres
	@echo "Storage services started"

up-jupyter:
	docker compose up -d jupyter
	@echo "Jupyter Lab started at http://localhost:8888"

# ==================== LOGS ====================

logs-kafka:
	docker compose logs -f kafka

logs-spark:
	docker compose logs -f spark-master spark-worker

logs-minio:
	docker compose logs -f minio

logs-postgres:
	docker compose logs -f postgres

logs-jupyter:
	docker compose logs -f jupyter

# ==================== UTILITIES ====================

kafka-topics:
	docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-create-topics:
	docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic events.raw.v1 --partitions 3 --replication-factor 1
	docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic events.processed.v1 --partitions 3 --replication-factor 1
	docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic events.dlq.v1 --partitions 1 --replication-factor 1
	@echo "Kafka topics created"

postgres-shell:
	docker compose exec postgres psql -U analytics_user -d analytics_warehouse

minio-shell:
	docker compose exec minio sh

spark-shell:
	docker compose exec spark-master /spark/bin/spark-shell

pyspark-shell:
	docker compose exec spark-master /spark/bin/pyspark
