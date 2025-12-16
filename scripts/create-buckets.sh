#!/bin/sh
# ============================================
# MinIO Bucket Initialization Script
# Creates all required buckets for the data lake
# ============================================

set -e

echo "============================================"
echo "MinIO Bucket Creation Script"
echo "============================================"

# Wait for MinIO to be fully ready
echo "Waiting for MinIO to be ready..."
sleep 5

# Configure MinIO client alias
echo "Configuring MinIO client..."
mc alias set myminio http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# Create buckets with error handling
create_bucket() {
    BUCKET_NAME=$1
    echo "Creating bucket: ${BUCKET_NAME}..."
    if mc ls myminio/${BUCKET_NAME} > /dev/null 2>&1; then
        echo "  Bucket '${BUCKET_NAME}' already exists, skipping."
    else
        mc mb myminio/${BUCKET_NAME}
        echo "  Bucket '${BUCKET_NAME}' created successfully."
    fi
}

# Create all required buckets
echo ""
echo "Creating data lake buckets..."
echo "--------------------------------------------"

create_bucket "${S3_BUCKET_RAW:-raw-events}"
create_bucket "${S3_BUCKET_BRONZE:-bronze}"
create_bucket "${S3_BUCKET_SILVER:-silver}"
create_bucket "${S3_BUCKET_GOLD:-gold}"

# Create additional buckets for specific use cases
create_bucket "checkpoints"
create_bucket "spark-events"
create_bucket "dead-letter-queue"

echo ""
echo "--------------------------------------------"
echo "Setting bucket policies..."

# Set public read policy for development (remove in production)
mc anonymous set download myminio/spark-events

echo ""
echo "============================================"
echo "MinIO bucket setup complete!"
echo "============================================"
echo ""
echo "Buckets created:"
mc ls myminio

exit 0
