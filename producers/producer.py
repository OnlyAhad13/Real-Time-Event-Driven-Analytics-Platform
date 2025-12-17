"""
Real-Time Analytics Platform - Kafka Event Producer
===================================================
Production-grade event producer using confluent-kafka.
Features:
- Schema versioning
- Idempotency keys  
- Configurable bad data injection for testing
- Graceful shutdown
- Metrics and logging
"""

import json
import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Callable

from confluent_kafka import Producer, KafkaError, KafkaException

from .config import Config, load_config
from .generator import EventGenerator


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("EventProducer")


class BadDataInjector:
    """
    Injects bad data into events for testing error handling.
    Simulates real-world data quality issues.
    """
    
    @staticmethod
    def remove_required_field(event: Dict[str, Any]) -> Dict[str, Any]:
        """Remove a random required field."""
        required_fields = ["event_id", "user_id", "timestamp", "event_type", "schema_version"]
        field_to_remove = random.choice(required_fields)
        event_copy = event.copy()
        event_copy.pop(field_to_remove, None)
        return event_copy
    
    @staticmethod
    def corrupt_timestamp(event: Dict[str, Any]) -> Dict[str, Any]:
        """Corrupt the timestamp field."""
        event_copy = event.copy()
        corruptions = [
            "not-a-timestamp",
            "2024-13-45T99:99:99Z",  # invalid date
            "",
            None,
            12345,  # wrong type
        ]
        event_copy["timestamp"] = random.choice(corruptions)
        return event_copy
    
    @staticmethod
    def corrupt_json_structure(event: Dict[str, Any]) -> str:
        """Return malformed JSON string."""
        corruptions = [
            '{"broken": "json",',  # unclosed
            'not json at all',
            '{"nested": {"too": {"deep":',
            '',
            '[]',  # array instead of object
        ]
        return random.choice(corruptions)
    
    @staticmethod
    def add_unexpected_fields(event: Dict[str, Any]) -> Dict[str, Any]:
        """Add unexpected/extra fields."""
        event_copy = event.copy()
        event_copy["__unexpected_field__"] = "should_not_be_here"
        event_copy["random_number"] = random.randint(1, 1000000)
        return event_copy
    
    @staticmethod
    def corrupt_payload(event: Dict[str, Any]) -> Dict[str, Any]:
        """Corrupt the payload field."""
        event_copy = event.copy()
        corruptions = [
            None,
            "string_instead_of_object",
            [],
            {"completely": "wrong", "structure": True},
        ]
        event_copy["payload"] = random.choice(corruptions)
        return event_copy
    
    @classmethod
    def inject_bad_data(cls, event: Dict[str, Any]) -> tuple[Any, str]:
        """
        Inject a random type of bad data.
        
        Returns:
            Tuple of (corrupted_event_or_string, corruption_type)
        """
        injectors = [
            (cls.remove_required_field, "missing_field"),
            (cls.corrupt_timestamp, "invalid_timestamp"),
            (cls.corrupt_json_structure, "malformed_json"),
            (cls.add_unexpected_fields, "extra_fields"),
            (cls.corrupt_payload, "invalid_payload"),
        ]
        
        injector, corruption_type = random.choice(injectors)
        return injector(event), corruption_type


class EventProducer:
    """
    Production-grade Kafka event producer using confluent-kafka.
    """
    
    def __init__(self, config: Optional[Config] = None):
        """
        Initialize the event producer.
        
        Args:
            config: Configuration object (loads from env if not provided)
        """
        self.config = config or load_config()
        self.generator = EventGenerator(
            num_users=self.config.producer.num_simulated_users
        )
        self.producer: Optional[Producer] = None
        self.running = False
        
        # Metrics
        self.events_sent = 0
        self.events_failed = 0
        self.events_delivered = 0
        self.bad_data_injected = 0
        self.start_time: Optional[float] = None
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Set log level
        log_level = getattr(logging, self.config.producer.log_level.upper(), logging.INFO)
        logger.setLevel(log_level)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
            self.events_failed += 1
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} "
                f"[partition={msg.partition()}, offset={msg.offset()}]"
            )
            self.events_delivered += 1
    
    def _create_producer(self) -> Producer:
        """Create and configure the Kafka producer."""
        kafka_config = self.config.kafka
        
        logger.info(f"Connecting to Kafka at {kafka_config.bootstrap_servers}")
        
        conf = {
            'bootstrap.servers': kafka_config.bootstrap_servers,
            'acks': kafka_config.acks,
            'retries': kafka_config.retries,
            'retry.backoff.ms': kafka_config.retry_backoff_ms,
            'batch.num.messages': 10000,  # librdkafka equivalent
            'linger.ms': kafka_config.linger_ms,
            'queue.buffering.max.kbytes': 32768,  # 32MB - librdkafka equivalent  
            'compression.type': kafka_config.compression_type,
            'message.timeout.ms': kafka_config.request_timeout_ms,
            # Enable idempotence for exactly-once semantics
            'enable.idempotence': True,
        }
        
        return Producer(conf)
    
    def connect(self, max_retries: int = 5, retry_delay: float = 2.0) -> bool:
        """
        Connect to Kafka with retry logic.
        
        Args:
            max_retries: Maximum connection attempts
            retry_delay: Delay between retries in seconds
        
        Returns:
            True if connected successfully
        """
        for attempt in range(max_retries):
            try:
                self.producer = self._create_producer()
                # Test connection by listing topics
                self.producer.list_topics(timeout=10)
                logger.info("Successfully connected to Kafka")
                return True
            except KafkaException as e:
                logger.warning(
                    f"Connection attempt {attempt + 1}/{max_retries} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        
        logger.error("Failed to connect to Kafka after all retries")
        return False
    
    def send_event(self, event: Dict[str, Any], inject_bad_data: bool = False) -> bool:
        """
        Send a single event to Kafka.
        
        Args:
            event: Event dictionary to send
            inject_bad_data: Whether to inject bad data
        
        Returns:
            True if sent successfully
        """
        if self.producer is None:
            logger.error("Producer not connected")
            return False
        
        try:
            # Determine event key for partitioning (use user_id for ordering)
            event_key = event.get("user_id", str(uuid.uuid4()))
            
            # Possibly inject bad data
            message_value = event
            if inject_bad_data:
                message_value, corruption_type = BadDataInjector.inject_bad_data(event)
                self.bad_data_injected += 1
                logger.debug(f"Injected bad data: {corruption_type}")
            
            # Serialize the message
            if isinstance(message_value, str):
                value_bytes = message_value.encode('utf-8')
            else:
                value_bytes = json.dumps(message_value).encode('utf-8')
            
            # Send to Kafka
            self.producer.produce(
                topic=self.config.kafka.topic,
                key=event_key.encode('utf-8'),
                value=value_bytes,
                callback=self._delivery_callback,
            )
            
            # Trigger delivery callbacks
            self.producer.poll(0)
            
            self.events_sent += 1
            return True
            
        except KafkaException as e:
            logger.error(f"Failed to send event: {e}")
            self.events_failed += 1
            return False
        except BufferError:
            logger.warning("Producer buffer full, waiting...")
            self.producer.poll(1)  # Wait for space
            return False
    
    def run(
        self,
        events_per_second: Optional[float] = None,
        total_events: Optional[int] = None,
        callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        """
        Run the event producer.
        
        Args:
            events_per_second: Events to generate per second (overrides config)
            total_events: Total events to generate (overrides config, None = infinite)
            callback: Optional callback function for each event
        """
        eps = events_per_second or self.config.producer.events_per_second
        total = total_events or self.config.producer.total_events
        bad_data_pct = self.config.producer.bad_data_percentage / 100.0
        
        if not self.connect():
            logger.error("Failed to connect to Kafka, exiting")
            sys.exit(1)
        
        self.running = True
        self.start_time = time.time()
        interval = 1.0 / eps if eps > 0 else 0
        
        logger.info(f"Starting event producer:")
        logger.info(f"  - Topic: {self.config.kafka.topic}")
        logger.info(f"  - Rate: {eps} events/second")
        logger.info(f"  - Total events: {total or 'infinite'}")
        logger.info(f"  - Bad data rate: {self.config.producer.bad_data_percentage}%")
        logger.info("-" * 50)
        
        try:
            events_generated = 0
            
            while self.running:
                # Check if we've reached the limit
                if total and events_generated >= total:
                    logger.info(f"Reached target of {total} events")
                    break
                
                # Generate event
                event = self.generator.generate_event(
                    distribution=self.config.producer.event_distribution
                )
                
                # Determine if we should inject bad data
                inject_bad = random.random() < bad_data_pct
                
                # Send event
                self.send_event(event, inject_bad_data=inject_bad)
                
                # Optional callback
                if callback:
                    callback(event)
                
                events_generated += 1
                
                # Log progress
                if events_generated % self.config.producer.log_every_n_events == 0:
                    elapsed = time.time() - self.start_time
                    rate = events_generated / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Progress: {events_generated} events sent "
                        f"({rate:.1f}/s, {self.bad_data_injected} bad, {self.events_failed} failed)"
                    )
                
                # Rate limiting
                if interval > 0:
                    time.sleep(interval)
        
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Gracefully shutdown the producer."""
        logger.info("Shutting down producer...")
        
        if self.producer:
            # Flush any remaining messages
            remaining = self.producer.flush(timeout=10)
            if remaining > 0:
                logger.warning(f"{remaining} messages were not delivered")
        
        # Print final stats
        elapsed = time.time() - self.start_time if self.start_time else 0
        logger.info("=" * 50)
        logger.info("Producer Statistics:")
        logger.info(f"  - Total events sent: {self.events_sent}")
        logger.info(f"  - Events delivered: {self.events_delivered}")
        logger.info(f"  - Events failed: {self.events_failed}")
        logger.info(f"  - Bad data injected: {self.bad_data_injected}")
        logger.info(f"  - Runtime: {elapsed:.1f} seconds")
        if elapsed > 0:
            logger.info(f"  - Average rate: {self.events_sent / elapsed:.1f} events/second")
        logger.info("=" * 50)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-Time Analytics Event Producer")
    parser.add_argument(
        "--events-per-second", "-e",
        type=float,
        default=None,
        help="Events to generate per second (default: from config)"
    )
    parser.add_argument(
        "--total-events", "-n",
        type=int,
        default=None,
        help="Total events to generate (default: infinite)"
    )
    parser.add_argument(
        "--bad-data-percentage", "-b",
        type=float,
        default=None,
        help="Percentage of bad data to inject (default: 1.0)"
    )
    parser.add_argument(
        "--bootstrap-servers", "-s",
        type=str,
        default=None,
        help="Kafka bootstrap servers (default: localhost:29093)"
    )
    parser.add_argument(
        "--topic", "-t",
        type=str,
        default=None,
        help="Kafka topic (default: events_raw)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Load config and apply CLI overrides
    config = load_config()
    
    if args.bootstrap_servers:
        config.kafka.bootstrap_servers = args.bootstrap_servers
    if args.topic:
        config.kafka.topic = args.topic
    if args.bad_data_percentage is not None:
        config.producer.bad_data_percentage = args.bad_data_percentage
    if args.verbose:
        config.producer.log_level = "DEBUG"
    
    # Create and run producer
    producer = EventProducer(config)
    producer.run(
        events_per_second=args.events_per_second,
        total_events=args.total_events,
    )


if __name__ == "__main__":
    main()
