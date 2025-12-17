"""
Real-Time Analytics Platform - Bronze Layer Ingestion
======================================================
Spark Structured Streaming job to ingest raw events from Kafka
and write to MinIO (S3-compatible) Bronze layer in Parquet format.

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.2 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minio_admin \
        --conf spark.hadoop.fs.s3a.secret.key=minio_password_change_me \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        streaming_job.py
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeIngestion")

# ============================================
# Configuration
# ============================================

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events_raw")

# MinIO/S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password_change_me")

# Output paths (using existing MinIO buckets)
BRONZE_OUTPUT_PATH = os.getenv("BRONZE_OUTPUT_PATH", "s3a://bronze/events/")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://checkpoints/events_bronze/")

# ============================================
# Schema Definition
# ============================================

# Schema for the raw event JSON
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("idempotency_key", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("geo_location", MapType(StringType(), StringType()), True),
    StructField("payload", MapType(StringType(), StringType()), True),
])

# ============================================
# Spark Session
# ============================================

def create_spark_session() -> SparkSession:
    """Create and configure SparkSession with S3A (MinIO) support."""
    logger.info("Creating SparkSession...")
    
    spark = (
        SparkSession.builder
        .appName("BronzeLayerIngestion")
        # S3A Configuration for MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Streaming configurations
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created: {spark.sparkContext.appName}")
    return spark

# ============================================
# Streaming Pipeline
# ============================================

def run_bronze_ingestion(spark: SparkSession):
    """
    Run the Bronze layer ingestion pipeline.
    
    Reads raw events from Kafka, parses JSON, adds date partition column,
    and writes to MinIO in Parquet format.
    """
    logger.info(f"Starting Bronze ingestion from Kafka topic: {KAFKA_TOPIC}")
    logger.info(f"Output path: {BRONZE_OUTPUT_PATH}")
    logger.info(f"Checkpoint path: {CHECKPOINT_PATH}")
    
    # Read from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    logger.info("Connected to Kafka stream")
    
    # Parse the Kafka message value (JSON string)
    parsed_df = (
        kafka_df
        .selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as raw_value", "timestamp as kafka_timestamp")
        .withColumn("event", from_json(col("raw_value"), EVENT_SCHEMA))
        .select(
            col("kafka_key"),
            col("kafka_timestamp"),
            col("raw_value"),
            col("event.*")
        )
    )
    
    # Add date partition column from timestamp
    partitioned_df = (
        parsed_df
        .withColumn("date", to_date(col("timestamp")))
        .withColumn("ingestion_time", current_timestamp())
    )
    
    logger.info("Stream transformations applied")
    
    # Write to Bronze layer (S3/MinIO)
    query = (
        partitioned_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("date", "event_type")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    logger.info(f"Streaming query started: {query.id}")
    logger.info(f"Query name: {query.name}")
    
    # Wait for termination (Ctrl+C or SIGTERM)
    query.awaitTermination()


# ============================================
# Main Entry Point
# ============================================

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Bronze Layer Ingestion - Starting")
    logger.info("=" * 60)
    
    spark = create_spark_session()
    
    try:
        run_bronze_ingestion(spark)
    except KeyboardInterrupt:
        logger.info("Received interrupt, stopping...")
    except Exception as e:
        logger.error(f"Error in Bronze ingestion: {e}")
        raise
    finally:
        spark.stop()
        logger.info("SparkSession stopped")
