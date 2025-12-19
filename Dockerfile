# =============================================================================
# Real-Time Analytics Platform - Master Dockerfile
# =============================================================================
# Multi-stage build for Python-based components:
#   - Event Producer
#   - Data Quality Checks
#   - Utility Scripts
#
# Usage:
#   docker build -t analytics-python:latest .
#   docker run --rm analytics-python:latest producer --total-events 1000
#   docker run --rm analytics-python:latest quality-check --date 2024-12-17
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Base Python Environment
# -----------------------------------------------------------------------------
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd --gid 1000 appgroup \
    && useradd --uid 1000 --gid appgroup --shell /bin/bash --create-home appuser

WORKDIR /app

# -----------------------------------------------------------------------------
# Stage 2: Dependencies
# -----------------------------------------------------------------------------
FROM base as dependencies

# Copy requirements files
COPY producers/requirements.txt /tmp/producer-requirements.txt
COPY scripts/requirements.txt /tmp/scripts-requirements.txt

# Install Python dependencies
RUN pip install --upgrade pip \
    && pip install -r /tmp/producer-requirements.txt \
    && pip install -r /tmp/scripts-requirements.txt \
    && pip install minio pandas pyarrow requests

# -----------------------------------------------------------------------------
# Stage 3: Application
# -----------------------------------------------------------------------------
FROM dependencies as application

# Copy application code
COPY producers/ /app/producers/
COPY scripts/ /app/scripts/
COPY config/ /app/config/

# Create entrypoint script
RUN cat > /app/entrypoint.sh << 'EOF'
#!/bin/bash
set -e

case "$1" in
    producer)
        shift
        exec python -m producers.producer "$@"
        ;;
    quality-check)
        shift
        exec python /app/scripts/data_quality_checks.py "$@"
        ;;
    init-kafka)
        shift
        exec python /app/scripts/init_kafka.py "$@"
        ;;
    shell)
        exec /bin/bash
        ;;
    *)
        exec "$@"
        ;;
esac
EOF

RUN chmod +x /app/entrypoint.sh

# Set ownership
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--help"]

# -----------------------------------------------------------------------------
# Labels (OCI Image Spec)
# -----------------------------------------------------------------------------
LABEL org.opencontainers.image.title="Real-Time Analytics Platform" \
      org.opencontainers.image.description="Python components for event-driven analytics" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.vendor="Analytics Team" \
      org.opencontainers.image.source="https://github.com/your-org/real-time-analytics"
