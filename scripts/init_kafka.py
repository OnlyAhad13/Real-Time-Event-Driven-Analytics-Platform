"""
Initialize Kafka topics for the Real-Time Analytics Platform.
"""

import sys
import logging
from confluent_kafka.admin import AdminClient, NewTopic, KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("KafkaInit")

def create_topics():
    """Create the required Kafka topics."""
    # Configuration
    bootstrap_servers = "localhost:29093"
    topics_to_create = [
        # Name, Partitions, Replication Factor
        NewTopic("events_raw", num_partitions=3, replication_factor=1),
        NewTopic("events_dead_letter_queue", num_partitions=1, replication_factor=1),
    ]

    logger.info(f"Connecting to Kafka at {bootstrap_servers}...")
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Create topics
    logger.info("Creating topics...")
    fs = admin_client.create_topics(topics_to_create)

    # Wait for operation to finish
    success = True
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info(f"Topic '{topic}' created successfully")
        except Exception as e:
            # Check if topic already exists
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.info(f"Topic '{topic}' already exists")
            else:
                logger.error(f"Failed to create topic '{topic}': {e}")
                success = False

    if success:
        logger.info("Kafka initialization complete!")
    else:
        logger.error("Some topics failed to create.")
        sys.exit(1)

if __name__ == "__main__":
    create_topics()
