# ============================================
# Event Producer Dockerfile
# ============================================

FROM python:3.11-slim

LABEL maintainer="Real-Time Analytics Platform"
LABEL description="Event producer for generating simulated business events"

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY producers/requirements.txt ./requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer code
COPY producers/ ./producers/

# Create non-root user
RUN useradd -m -u 1000 producer
USER producer

# Environment variables (can be overridden)
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=events_raw
ENV EVENTS_PER_SECOND=10
ENV BAD_DATA_PERCENTAGE=1.0
ENV LOG_LEVEL=INFO

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import producers; print('OK')" || exit 1

# Run the producer
ENTRYPOINT ["python", "-m", "producers.producer"]

# Default arguments (can be overridden with docker run)
CMD []
