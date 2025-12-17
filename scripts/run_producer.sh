# ============================================
# Run Event Producer
# ============================================
# Usage: ./run_producer.sh [options]
# Options are passed directly to the producer
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load environment variables if .env eexists
if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

# Default values
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29093}"
KAFKA_TOPIC="${KAFKA_TOPIC:-events_raw}"

echo "============================================"
echo "Real-Time Analytics - Event Producer"
echo "============================================"
echo "Kafka: $KAFKA_BOOTSTRAP_SERVERS"
echo "Topic: $KAFKA_TOPIC"
echo "============================================"
echo ""

# Check if running in Docker or locally
if [ -f /.dockerenv ]; then
    # Running inside Docker
    python -m producers.producer "$@"
else
    # Running locally
    cd "$PROJECT_ROOT"
    
    # Check if virtual environment exists
    if [ -d "venv" ]; then
        source venv/bin/activate
    elif [ -d ".venv" ]; then
        source .venv/bin/activate
    fi
    
    # Run the producer
    python -m producers.producer \
        --bootstrap-servers "$KAFKA_BOOTSTRAP_SERVERS" \
        --topic "$KAFKA_TOPIC" \
        "$@"
fi
