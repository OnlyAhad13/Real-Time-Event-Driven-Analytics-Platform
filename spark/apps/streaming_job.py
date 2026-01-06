"""
Real-Time Analytics Platform - Bronze & Silver Layer Streaming
===============================================================
Spark Structured Streaming job for:
  - Bronze: Raw ingestion from Kafka to Parquet
  - Silver: Schema validation, deduplication, sessionization

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.2 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minio_admin \
        --conf spark.hadoop.fs.s3a.secret.key=minio_password_change_me \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        streaming_job.py [bronze|silver|both]
"""

import os
import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_date, current_timestamp, to_timestamp,
    window, count, collect_list, first, lit, when, coalesce,
    min as spark_min, max as spark_max, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StreamingJob")

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
BRONZE_CHECKPOINT_PATH = os.getenv("BRONZE_CHECKPOINT_PATH", "s3a://checkpoints/events_bronze/")

SILVER_OUTPUT_PATH = os.getenv("SILVER_OUTPUT_PATH", "s3a://silver/events/")
SILVER_CHECKPOINT_PATH = os.getenv("SILVER_CHECKPOINT_PATH", "s3a://checkpoints/events_silver/")

DLQ_OUTPUT_PATH = os.getenv("DLQ_OUTPUT_PATH", "s3a://dead-letter-queue/events/")
DLQ_CHECKPOINT_PATH = os.getenv("DLQ_CHECKPOINT_PATH", "s3a://checkpoints/events_dlq/")

# Processing Configuration
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")
SESSION_WINDOW = os.getenv("SESSION_WINDOW", "30 minutes")

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

# Required fields for schema validation
REQUIRED_FIELDS = ["event_id", "user_id", "timestamp", "event_type", "idempotency_key"]

# ============================================
# Spark Session
# ============================================

def create_spark_session(app_name: str = "StreamingJob") -> SparkSession:
    """Create and configure SparkSession with S3A (MinIO) support."""
    logger.info("Creating SparkSession...")
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        # S3A Configuration for MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Streaming configurations
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created: {spark.sparkContext.appName}")
    return spark

# ============================================
# Bronze Layer Pipeline
# ============================================

def run_bronze_ingestion(spark: SparkSession):
    """
    Bronze Layer: Raw ingestion from Kafka to Parquet.
    
    Reads raw events from Kafka, parses JSON, adds date partition column,
    and writes to MinIO in Parquet format.
    """
    logger.info("=" * 60)
    logger.info("BRONZE LAYER - Starting Raw Ingestion")
    logger.info("=" * 60)
    logger.info(f"Kafka topic: {KAFKA_TOPIC}")
    logger.info(f"Output path: {BRONZE_OUTPUT_PATH}")
    logger.info(f"Checkpoint path: {BRONZE_CHECKPOINT_PATH}")
    
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
        .selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as raw_value",
            "timestamp as kafka_timestamp"
        )
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
        .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
        .partitionBy("date", "event_type")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    logger.info(f"Bronze streaming query started: {query.id}")
    return query

# ============================================
# Silver Layer Pipeline
# ============================================

def validate_schema(df: DataFrame) -> tuple:
    """
    Validate schema and split into valid/invalid records.
    
    Returns:
        Tuple of (valid_df, invalid_df)
    """
    # Check for required fields being non-null
    validation_condition = (
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("timestamp").isNotNull() &
        col("event_type").isNotNull() &
        col("idempotency_key").isNotNull()
    )
    
    valid_df = df.filter(validation_condition)
    invalid_df = df.filter(~validation_condition)
    
    return valid_df, invalid_df


def run_silver_processing(spark: SparkSession):
    """
    Silver Layer: Schema validation, deduplication, and sessionization.
    
    Reads from Bronze layer (Parquet) and applies transformations:
    - Schema validation with DLQ routing
    - Watermarking for late data (10 minutes)
    - Deduplication by idempotency_key
    - Sessionization with 30-minute windows by user_id
    """
    logger.info("=" * 60)
    logger.info("SILVER LAYER - Starting Processing")
    logger.info("=" * 60)
    logger.info(f"Reading from Bronze: {BRONZE_OUTPUT_PATH}")
    logger.info(f"Output path: {SILVER_OUTPUT_PATH}")
    logger.info(f"DLQ path: {DLQ_OUTPUT_PATH}")
    logger.info(f"Watermark delay: {WATERMARK_DELAY}")
    logger.info(f"Session window: {SESSION_WINDOW}")
    
    # -----------------------------------------
    # Read from Bronze Layer (Parquet)
    # -----------------------------------------
    bronze_df = (
        spark.readStream
        .format("parquet")
        .schema(StructType([
            StructField("kafka_key", StringType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
            StructField("raw_value", StringType(), True),
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
            StructField("date", StringType(), True),
            StructField("ingestion_time", TimestampType(), True),
        ]))
        .option("path", BRONZE_OUTPUT_PATH)
        .load()
    )
    
    logger.info("Connected to Bronze Parquet stream")
    
    # -----------------------------------------
    # Step 1: Schema Validation
    # -----------------------------------------
    logger.info("Applying schema validation...")
    
    # Add validation flag
    validated_df = bronze_df.withColumn(
        "is_valid",
        when(
            col("event_id").isNotNull() &
            col("user_id").isNotNull() &
            col("timestamp").isNotNull() &
            col("event_type").isNotNull() &
            col("idempotency_key").isNotNull(),
            lit(True)
        ).otherwise(lit(False))
    )
    
    # Split into valid and invalid streams
    valid_df = validated_df.filter(col("is_valid") == True).drop("is_valid")
    invalid_df = validated_df.filter(col("is_valid") == False).drop("is_valid")
    
    # -----------------------------------------
    # Step 2: Write Invalid Records to DLQ
    # -----------------------------------------
    logger.info("Setting up DLQ stream...")
    
    dlq_df = (
        invalid_df
        .withColumn("dlq_reason", lit("schema_validation_failed"))
        .withColumn("dlq_timestamp", current_timestamp())
    )
    
    dlq_query = (
        dlq_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", DLQ_OUTPUT_PATH)
        .option("checkpointLocation", DLQ_CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    logger.info(f"DLQ streaming query started: {dlq_query.id}")
    
    # -----------------------------------------
    # Step 3: Watermarking for Late Data
    # -----------------------------------------
    logger.info(f"Applying watermark: {WATERMARK_DELAY}")
    
    # Convert timestamp string to timestamp type and apply watermark
    watermarked_df = (
        valid_df
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withWatermark("event_timestamp", WATERMARK_DELAY)
    )
    
    # -----------------------------------------
    # Step 4: Deduplication by idempotency_key
    # -----------------------------------------
    logger.info("Applying deduplication by idempotency_key...")
    
    deduplicated_df = watermarked_df.dropDuplicates(["idempotency_key"])
    
    # -----------------------------------------
    # Step 5: Sessionization (30-minute window by user_id)
    # -----------------------------------------
    logger.info(f"Applying sessionization: {SESSION_WINDOW} window by user_id")
    
    # Add session window and aggregate
    sessionized_df = (
        deduplicated_df
        .groupBy(
            col("user_id"),
            window(col("event_timestamp"), SESSION_WINDOW)
        )
        .agg(
            count("*").alias("event_count"),
            spark_min("event_timestamp").alias("session_start"),
            spark_max("event_timestamp").alias("session_end"),
            collect_list("event_type").alias("event_types"),
            collect_list("event_id").alias("event_ids"),
            first("device_type").alias("primary_device"),
            first("ip_address").alias("primary_ip"),
            first("geo_location").alias("primary_geo"),
        )
        .select(
            col("user_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_count"),
            col("session_start"),
            col("session_end"),
            col("event_types"),
            col("event_ids"),
            col("primary_device"),
            col("primary_ip"),
            col("primary_geo"),
            current_timestamp().alias("processed_at")
        )
    )
    
    # Add date partition column
    final_df = sessionized_df.withColumn("date", to_date(col("window_start")))
    
    # -----------------------------------------
    # Step 6: Write to Silver Layer
    # -----------------------------------------
    logger.info("Writing to Silver layer...")
    
    silver_query = (
        final_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_OUTPUT_PATH)
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .partitionBy("date")
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    logger.info(f"Silver streaming query started: {silver_query.id}")
    
    logger.info(f"Silver streaming query started: {silver_query.id}")

    # -----------------------------------------
    # Step 7: Write to Kafka for Real-Time Dashboard
    # -----------------------------------------
    logger.info("Writing to Kafka (events.processed.v1) for Dashboard...")
    
    # Use valid_df for low-latency feed (before sessionization)
    kafka_output_df = (
        valid_df
        .selectExpr("CAST(user_id AS STRING) AS key", "to_json(struct(*)) AS value")
    )
    
    kafka_query = (
        kafka_output_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", "events.processed.v1")
        .option("checkpointLocation", "s3a://checkpoints/events_processed_kafka/")
        .trigger(processingTime="1 seconds") 
        .start()
    )
    
    return dlq_query, silver_query, kafka_query

# ============================================
# Main Entry Point
# ============================================

def main():
    """Main entry point."""
    # Parse command line argument
    mode = "both"  # Default mode
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    
    valid_modes = ["bronze", "silver", "both"]
    if mode not in valid_modes:
        logger.error(f"Invalid mode: {mode}. Must be one of: {valid_modes}")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info(f"Streaming Job - Mode: {mode.upper()}")
    logger.info("=" * 60)
    
    app_name = f"StreamingJob-{mode.capitalize()}"
    spark = create_spark_session(app_name)
    
    queries = []
    
    try:
        if mode in ["bronze", "both"]:
            bronze_query = run_bronze_ingestion(spark)
            queries.append(bronze_query)
        
        if mode in ["silver", "both"]:
            dlq_query, silver_query, kafka_query = run_silver_processing(spark)
            queries.extend([dlq_query, silver_query, kafka_query])
        
        # Wait for all queries
        logger.info(f"Waiting for {len(queries)} streaming queries...")
        for query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        logger.info("Received interrupt, stopping...")
    except Exception as e:
        logger.error(f"Error in streaming job: {e}")
        raise
    finally:
        # Stop all active queries
        for query in queries:
            try:
                query.stop()
            except:
                pass
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
