"""
Real-Time Analytics Platform - Batch Loader
=============================================
Daily batch job to load Silver layer data into the Data Warehouse.

Features:
- Reads previous day's data from Silver layer (S3/MinIO)
- Incremental load into fact_events table
- SCD Type-2 updates for dim_user table

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.6.0 \
        batch_loader.py [--date YYYY-MM-DD]
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, to_date,
    when, coalesce, struct, row_number, max as spark_max
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, BooleanType, DecimalType, ArrayType, MapType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BatchLoader")

# ============================================
# Configuration
# ============================================

# MinIO/S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password_change_me")

# Data Lake Paths
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://silver/events/")

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics_warehouse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "analytics_password_change_me")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ============================================
# Schema Definitions
# ============================================

SILVER_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("window_start", TimestampType(), True),
    StructField("window_end", TimestampType(), True),
    StructField("event_count", IntegerType(), True),
    StructField("session_start", TimestampType(), True),
    StructField("session_end", TimestampType(), True),
    StructField("event_types", ArrayType(StringType()), True),
    StructField("event_ids", ArrayType(StringType()), True),
    StructField("primary_device", StringType(), True),
    StructField("primary_ip", StringType(), True),
    StructField("primary_geo", MapType(StringType(), StringType()), True),
    StructField("processed_at", TimestampType(), True),
    StructField("date", StringType(), True),
])

# ============================================
# Spark Session
# ============================================

def create_spark_session() -> SparkSession:
    """Create SparkSession with S3A and JDBC support."""
    logger.info("Creating SparkSession...")
    
    spark = (
        SparkSession.builder
        .appName("BatchLoader")
        # S3A Configuration for MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Shuffle partitions
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created: {spark.sparkContext.appName}")
    return spark

# ============================================
# Data Reading Functions
# ============================================

def read_silver_data(spark: SparkSession, target_date: str) -> DataFrame:
    """
    Read Silver layer data for a specific date.
    
    Args:
        spark: SparkSession
        target_date: Date string in YYYY-MM-DD format
    
    Returns:
        DataFrame with Silver data for the specified date
    """
    silver_path = f"{SILVER_PATH}date={target_date}/"
    logger.info(f"Reading Silver data from: {silver_path}")
    
    try:
        df = spark.read.parquet(silver_path)
        row_count = df.count()
        logger.info(f"Read {row_count} rows from Silver layer")
        return df
    except Exception as e:
        logger.warning(f"No data found for date {target_date}: {e}")
        return spark.createDataFrame([], SILVER_SCHEMA)

def read_dim_user(spark: SparkSession) -> DataFrame:
    """Read current dim_user data from PostgreSQL."""
    logger.info("Reading dim_user from PostgreSQL...")
    
    df = (
        spark.read
        .jdbc(JDBC_URL, "dim_user", properties=JDBC_PROPERTIES)
    )
    
    logger.info(f"Read {df.count()} rows from dim_user")
    return df

def read_dim_device(spark: SparkSession) -> DataFrame:
    """Read dim_device data from PostgreSQL."""
    logger.info("Reading dim_device from PostgreSQL...")
    
    return (
        spark.read
        .jdbc(JDBC_URL, "dim_device", properties=JDBC_PROPERTIES)
    )

def read_dim_time(spark: SparkSession) -> DataFrame:
    """Read dim_time data from PostgreSQL."""
    logger.info("Reading dim_time from PostgreSQL...")
    
    return (
        spark.read
        .jdbc(JDBC_URL, "dim_time", properties=JDBC_PROPERTIES)
    )

# ============================================
# SCD Type-2 Logic for dim_user
# ============================================

def apply_scd_type2_user(spark: SparkSession, silver_df: DataFrame, current_date: str):
    """
    Apply SCD Type-2 logic to dim_user.
    
    This function:
    1. Identifies new users to insert
    2. Identifies existing users with changed attributes
    3. Expires old records and inserts new versions
    
    Tracked attributes for SCD-2:
    - plan_type (extracted from events)
    - subscription_status
    """
    logger.info("=" * 60)
    logger.info("Applying SCD Type-2 to dim_user")
    logger.info("=" * 60)
    
    # Extract unique users from Silver data
    # Note: In a real scenario, user attributes would come from a dedicated user events stream
    # Here we're simulating by extracting user_id and device info
    
    users_from_silver = (
        silver_df
        .select(
            col("user_id"),
            col("primary_device").alias("device_type"),
            col("primary_geo").getItem("country").alias("country"),
            col("primary_geo").getItem("region").alias("region"),
            col("primary_geo").getItem("city").alias("city"),
        )
        .distinct()
    )
    
    # Get current users from dimension
    current_users = read_dim_user(spark).filter(col("is_current") == True)
    
    # Find new users (not in current dimension)
    new_users = (
        users_from_silver.alias("silver")
        .join(
            current_users.alias("dim"),
            col("silver.user_id") == col("dim.user_id"),
            "left_anti"
        )
    )
    
    new_user_count = new_users.count()
    logger.info(f"Found {new_user_count} new users to insert")
    
    if new_user_count > 0:
        # Prepare new user records
        new_user_records = (
            new_users
            .select(
                col("user_id"),
                lit(None).alias("email"),
                lit(None).alias("username"),
                lit("free").alias("plan_type"),  # Default plan
                lit("active").alias("subscription_status"),
                lit("bronze").alias("user_tier"),  # Default tier
                col("country"),
                col("region"),
                col("city"),
                lit(None).alias("timezone"),
                to_date(lit(current_date)).alias("signup_date"),
                lit("organic").alias("signup_source"),
                to_timestamp(lit(current_date)).alias("effective_start_date"),
                lit(None).cast(TimestampType()).alias("effective_end_date"),
                lit(True).alias("is_current"),
                lit(1).alias("version_number"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at"),
            )
        )
        
        # Write new users to dimension
        (
            new_user_records
            .write
            .jdbc(JDBC_URL, "dim_user", mode="append", properties=JDBC_PROPERTIES)
        )
        
        logger.info(f"Inserted {new_user_count} new users into dim_user")
    
    # Check for attribute changes (simulated - in real scenario, compare actual attributes)
    # For demo purposes, we'll check if any users need plan upgrades based on event activity
    
    # Find users with high activity (potential plan upgrades)
    high_activity_users = (
        silver_df
        .groupBy("user_id")
        .count()
        .filter(col("count") > 5)  # Users with more than 5 sessions
        .select("user_id")
    )
    
    # Get current "free" plan users who have high activity
    users_to_upgrade = (
        current_users
        .filter(col("plan_type") == "free")
        .join(high_activity_users, "user_id", "inner")
    )
    
    upgrade_count = users_to_upgrade.count()
    logger.info(f"Found {upgrade_count} users eligible for plan upgrade (SCD-2)")
    
    if upgrade_count > 0:
        # For SCD-2 updates, we need to:
        # 1. Expire the current record (set is_current=FALSE, set effective_end_date)
        # 2. Insert a new record with the updated attributes
        
        # Get user_keys to expire
        user_keys_to_expire = [row.user_key for row in users_to_upgrade.collect()]
        
        if user_keys_to_expire:
            # Execute UPDATE via JDBC (using raw SQL)
            # Note: PySpark doesn't support direct updates, so we use JDBC connection
            
            import psycopg2
            
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            
            try:
                cursor = conn.cursor()
                
                # Expire old records
                expire_sql = f"""
                    UPDATE dim_user 
                    SET is_current = FALSE, 
                        effective_end_date = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_key IN ({','.join(map(str, user_keys_to_expire))})
                """
                cursor.execute(expire_sql)
                
                conn.commit()
                logger.info(f"Expired {len(user_keys_to_expire)} old user records")
                
            finally:
                cursor.close()
                conn.close()
            
            # Insert new versions with updated plan
            new_versions = (
                users_to_upgrade
                .select(
                    col("user_id"),
                    col("email"),
                    col("username"),
                    lit("basic").alias("plan_type"),  # Upgraded plan
                    col("subscription_status"),
                    lit("silver").alias("user_tier"),  # Upgraded tier
                    col("country"),
                    col("region"),
                    col("city"),
                    col("timezone"),
                    col("signup_date"),
                    col("signup_source"),
                    current_timestamp().alias("effective_start_date"),
                    lit(None).cast(TimestampType()).alias("effective_end_date"),
                    lit(True).alias("is_current"),
                    (col("version_number") + 1).alias("version_number"),
                    current_timestamp().alias("created_at"),
                    current_timestamp().alias("updated_at"),
                )
            )
            
            (
                new_versions
                .write
                .jdbc(JDBC_URL, "dim_user", mode="append", properties=JDBC_PROPERTIES)
            )
            
            logger.info(f"Inserted {upgrade_count} new user versions (upgraded to basic plan)")
    
    logger.info("SCD Type-2 processing complete")

# ============================================
# Fact Table Loading
# ============================================

def load_fact_events(spark: SparkSession, silver_df: DataFrame, target_date: str):
    """
    Load fact_events table with incremental data from Silver layer.
    
    This performs:
    1. Dimension key lookups
    2. Fact record transformation
    3. Incremental insert (avoiding duplicates via idempotency_key)
    """
    logger.info("=" * 60)
    logger.info("Loading fact_events")
    logger.info("=" * 60)
    
    if silver_df.count() == 0:
        logger.info("No data to load into fact_events")
        return
    
    # Read dimension tables for key lookups
    dim_user = read_dim_user(spark).filter(col("is_current") == True)
    dim_device = read_dim_device(spark)
    dim_time = read_dim_time(spark)
    
    # Explode Silver sessions into individual events
    # Silver has aggregated session data, we need to create fact records
    from pyspark.sql.functions import explode, arrays_zip
    
    events_df = (
        silver_df
        .select(
            col("user_id"),
            col("session_start").alias("event_timestamp"),
            col("primary_device").alias("device_type"),
            col("primary_ip").alias("ip_address"),
            col("primary_geo"),
            col("event_count"),
            explode(arrays_zip(col("event_ids"), col("event_types"))).alias("event_data")
        )
        .select(
            col("user_id"),
            col("event_timestamp"),
            col("device_type"),
            col("ip_address"),
            col("primary_geo"),
            col("event_count"),
            col("event_data.event_ids").alias("event_id"),
            col("event_data.event_types").alias("event_type"),
        )
    )
    
    # Join with dim_user for user_key
    events_with_user = (
        events_df.alias("e")
        .join(
            dim_user.select("user_key", "user_id").alias("u"),
            col("e.user_id") == col("u.user_id"),
            "left"
        )
        .select(
            col("e.*"),
            col("u.user_key")
        )
    )
    
    # Join with dim_device for device_key
    events_with_device = (
        events_with_user.alias("e")
        .join(
            dim_device.select("device_key", "device_type").alias("d"),
            col("e.device_type") == col("d.device_type"),
            "left"
        )
        .select(
            col("e.*"),
            col("d.device_key")
        )
    )
    
    # Join with dim_time for time_key (truncate to hour)
    from pyspark.sql.functions import date_trunc
    
    events_with_time = (
        events_with_device
        .withColumn("event_hour", date_trunc("hour", col("event_timestamp")))
    )
    
    events_with_time_key = (
        events_with_time.alias("e")
        .join(
            dim_time.select("time_key", "full_timestamp").alias("t"),
            col("e.event_hour") == col("t.full_timestamp"),
            "left"
        )
        .select(
            col("e.*"),
            col("t.time_key")
        )
    )
    
    # Prepare final fact records
    fact_records = (
        events_with_time_key
        .select(
            col("event_id"),
            col("event_id").alias("idempotency_key"),  # Using event_id as idempotency
            col("user_key"),
            col("time_key"),
            col("device_key"),
            col("event_type"),
            lit("v1").alias("schema_version"),
            lit(None).alias("session_id"),
            col("ip_address"),
            col("primary_geo").getItem("country").alias("geo_country"),
            col("primary_geo").getItem("region").alias("geo_region"),
            col("primary_geo").getItem("city").alias("geo_city"),
            col("primary_geo").getItem("lat").cast(DecimalType(10, 8)).alias("geo_latitude"),
            col("primary_geo").getItem("lon").cast(DecimalType(11, 8)).alias("geo_longitude"),
            when(col("event_type") == "page_view", 1).otherwise(0).alias("page_view_count"),
            when(col("event_type") == "purchase", lit(99.99)).otherwise(lit(0)).alias("purchase_amount"),
            when(col("event_type") == "purchase", 1).otherwise(0).alias("purchase_quantity"),
            when(col("event_type") == "purchase", True).otherwise(False).alias("is_conversion"),
            when(col("event_type") == "payment_failed", True).otherwise(False).alias("is_error"),
            lit(None).alias("payload_json"),
            col("event_timestamp"),
            lit(None).cast(TimestampType()).alias("kafka_timestamp"),
            lit(None).cast(TimestampType()).alias("ingestion_timestamp"),
            current_timestamp().alias("warehouse_loaded_at"),
            lit(1.0).cast(DecimalType(3, 2)).alias("data_quality_score"),
        )
    )
    
    # Write to fact_events (append mode with ON CONFLICT handling)
    row_count = fact_records.count()
    logger.info(f"Preparing to load {row_count} rows into fact_events")
    
    # Use temporary table approach for upsert
    temp_table_name = f"temp_fact_events_{target_date.replace('-', '')}"
    
    (
        fact_records
        .write
        .jdbc(JDBC_URL, temp_table_name, mode="overwrite", properties=JDBC_PROPERTIES)
    )
    
    logger.info(f"Wrote {row_count} rows to temporary table {temp_table_name}")
    
    # Execute INSERT ON CONFLICT (upsert) via raw SQL
    import psycopg2
    
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    
    try:
        cursor = conn.cursor()
        
        # Upsert from temp table to fact_events
        upsert_sql = f"""
            INSERT INTO fact_events (
                event_id, idempotency_key, user_key, time_key, device_key,
                event_type, schema_version, session_id, ip_address,
                geo_country, geo_region, geo_city, geo_latitude, geo_longitude,
                page_view_count, purchase_amount, purchase_quantity,
                is_conversion, is_error, payload_json,
                event_timestamp, kafka_timestamp, ingestion_timestamp,
                warehouse_loaded_at, data_quality_score
            )
            SELECT 
                event_id, idempotency_key, user_key, time_key, device_key,
                event_type, schema_version, session_id, ip_address,
                geo_country, geo_region, geo_city, geo_latitude, geo_longitude,
                page_view_count, purchase_amount, purchase_quantity,
                is_conversion, is_error, payload_json::jsonb,
                event_timestamp, kafka_timestamp, ingestion_timestamp,
                warehouse_loaded_at, data_quality_score
            FROM {temp_table_name}
            ON CONFLICT (idempotency_key) DO NOTHING;
        """
        cursor.execute(upsert_sql)
        rows_inserted = cursor.rowcount
        
        # Drop temp table
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        
        conn.commit()
        logger.info(f"Inserted {rows_inserted} new rows into fact_events (skipped duplicates)")
        
    finally:
        cursor.close()
        conn.close()

# ============================================
# Main Entry Point
# ============================================

def main():
    """Main entry point for batch loader."""
    parser = argparse.ArgumentParser(description="Batch Loader: Silver to Warehouse ETL")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date in YYYY-MM-DD format (default: yesterday)"
    )
    args = parser.parse_args()
    
    # Determine target date
    if args.date:
        target_date = args.date
    else:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info("=" * 60)
    logger.info("Batch Loader - Starting")
    logger.info(f"Target Date: {target_date}")
    logger.info("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Read Silver data for target date
        silver_df = read_silver_data(spark, target_date)
        
        if silver_df.count() == 0:
            logger.warning(f"No Silver data found for {target_date}. Exiting.")
            return
        
        # Step 1: Apply SCD Type-2 updates to dim_user
        apply_scd_type2_user(spark, silver_df, target_date)
        
        # Step 2: Load fact_events with incremental insert
        load_fact_events(spark, silver_df, target_date)
        
        logger.info("=" * 60)
        logger.info("Batch Loader - Complete")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error in batch loader: {e}")
        raise
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
