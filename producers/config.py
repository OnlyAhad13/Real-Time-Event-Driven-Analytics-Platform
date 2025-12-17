"""
Real-Time Analytics Platform - Producer Configuration
=====================================================
Centralized configuration for the event producer.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class KafkaConfig:
    """Kafka producer configuration."""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29093")
    topic: str = os.getenv("KAFKA_TOPIC", "events_raw")
    
    # Producer settings
    acks: str = os.getenv("KAFKA_ACKS", "all")  # all = wait for all replicas
    retries: int = int(os.getenv("KAFKA_RETRIES", "3"))
    retry_backoff_ms: int = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "100"))
    
    # Batching settings
    batch_size: int = int(os.getenv("KAFKA_BATCH_SIZE", "16384"))  # 16KB
    linger_ms: int = int(os.getenv("KAFKA_LINGER_MS", "5"))
    buffer_memory: int = int(os.getenv("KAFKA_BUFFER_MEMORY", "33554432"))  # 32MB
    
    # Compression
    compression_type: str = os.getenv("KAFKA_COMPRESSION_TYPE", "gzip")
    
    # Timeouts
    request_timeout_ms: int = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000"))
    delivery_timeout_ms: int = int(os.getenv("KAFKA_DELIVERY_TIMEOUT_MS", "120000"))


@dataclass
class ProducerConfig:
    """Event producer configuration."""
    # Event generation settings
    events_per_second: float = float(os.getenv("EVENTS_PER_SECOND", "10"))
    total_events: Optional[int] = None  # None = run indefinitely
    
    # Bad data injection for testing error handling
    bad_data_percentage: float = float(os.getenv("BAD_DATA_PERCENTAGE", "1.0"))  # 1%
    
    # Event type distribution (must sum to 100)
    event_distribution: dict = None
    
    # User simulation
    num_simulated_users: int = int(os.getenv("NUM_SIMULATED_USERS", "1000"))
    
    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_every_n_events: int = int(os.getenv("LOG_EVERY_N_EVENTS", "100"))
    
    def __post_init__(self):
        if self.event_distribution is None:
            self.event_distribution = {
                "page_view": 60,      # 60% - most common
                "user_signup": 10,    # 10% - new users
                "purchase": 20,       # 20% - conversions
                "payment_failed": 10  # 10% - errors
            }
        
        # Parse total_events from env if set
        total_events_env = os.getenv("TOTAL_EVENTS")
        if total_events_env:
            self.total_events = int(total_events_env)


@dataclass
class Config:
    """Main configuration container."""
    kafka: KafkaConfig = None
    producer: ProducerConfig = None
    
    def __post_init__(self):
        if self.kafka is None:
            self.kafka = KafkaConfig()
        if self.producer is None:
            self.producer = ProducerConfig()


def load_config() -> Config:
    """Load configuration from environment variables."""
    return Config()
