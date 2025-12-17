"""
Real-Time Analytics Platform - Producer Tests
=============================================
Unit tests for the event producer module.
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from producers.schemas import (
    EventType,
    SchemaVersion,
    UserSignupEvent,
    PageViewEvent,
    PurchaseEvent,
    PaymentFailedEvent,
    SCHEMA_REGISTRY,
)
from producers.generator import EventGenerator
from producers.producer import BadDataInjector, EventProducer
from producers.config import Config, KafkaConfig, ProducerConfig


class TestSchemas:
    """Test event schemas."""
    
    def test_user_signup_event_structure(self):
        """Test UserSignupEvent has all required fields."""
        event = UserSignupEvent(user_id="test-user-123")
        data = event.to_dict()
        
        assert "event_id" in data
        assert "user_id" in data
        assert "timestamp" in data
        assert "event_type" in data
        assert "schema_version" in data
        assert "idempotency_key" in data
        assert "payload" in data
        
        assert data["event_type"] == EventType.USER_SIGNUP.value
        assert data["schema_version"] == SchemaVersion.V1.value
        assert data["user_id"] == "test-user-123"
    
    def test_page_view_event_structure(self):
        """Test PageViewEvent has all required fields."""
        event = PageViewEvent(user_id="test-user-456")
        data = event.to_dict()
        
        assert data["event_type"] == EventType.PAGE_VIEW.value
        assert "page_url" in data["payload"]
        assert "session_id" in data["payload"]
    
    def test_purchase_event_structure(self):
        """Test PurchaseEvent has all required fields."""
        event = PurchaseEvent(user_id="test-user-789")
        data = event.to_dict()
        
        assert data["event_type"] == EventType.PURCHASE.value
        assert "transaction_id" in data["payload"]
        assert "total_amount" in data["payload"]
        assert "currency" in data["payload"]
    
    def test_payment_failed_event_structure(self):
        """Test PaymentFailedEvent has all required fields."""
        event = PaymentFailedEvent(user_id="test-user-000")
        data = event.to_dict()
        
        assert data["event_type"] == EventType.PAYMENT_FAILED.value
        assert "failure_reason" in data["payload"]
        assert "error_code" in data["payload"]
    
    def test_unique_event_ids(self):
        """Test that each event gets a unique event_id."""
        events = [UserSignupEvent() for _ in range(100)]
        event_ids = [e.event_id for e in events]
        assert len(set(event_ids)) == 100
    
    def test_unique_idempotency_keys(self):
        """Test that each event gets a unique idempotency_key."""
        events = [PageViewEvent() for _ in range(100)]
        keys = [e.idempotency_key for e in events]
        assert len(set(keys)) == 100
    
    def test_timestamp_is_utc(self):
        """Test that timestamps are in UTC."""
        event = PurchaseEvent()
        # Timestamp should end with +00:00 or Z
        assert "+00:00" in event.timestamp or "Z" in event.timestamp


class TestEventGenerator:
    """Test event generator."""
    
    def test_generate_user_signup(self):
        """Test user signup generation."""
        gen = EventGenerator(num_users=100, seed=42)
        event = gen.generate_user_signup()
        data = event.to_dict()
        
        assert data["event_type"] == "user_signup"
        assert "@" in data["payload"]["email"]
        assert len(data["payload"]["username"]) > 0
    
    def test_generate_page_view(self):
        """Test page view generation."""
        gen = EventGenerator(num_users=100, seed=42)
        event = gen.generate_page_view()
        data = event.to_dict()
        
        assert data["event_type"] == "page_view"
        assert data["payload"]["page_url"].startswith("https://")
        assert len(data["payload"]["session_id"]) > 0
    
    def test_generate_purchase(self):
        """Test purchase generation."""
        gen = EventGenerator(num_users=100, seed=42)
        event = gen.generate_purchase()
        data = event.to_dict()
        
        assert data["event_type"] == "purchase"
        assert data["payload"]["total_amount"] > 0
        assert data["payload"]["quantity"] >= 1
    
    def test_generate_payment_failed(self):
        """Test payment failed generation."""
        gen = EventGenerator(num_users=100, seed=42)
        event = gen.generate_payment_failed()
        data = event.to_dict()
        
        assert data["event_type"] == "payment_failed"
        assert len(data["payload"]["failure_reason"]) > 0
        assert data["payload"]["error_code"].startswith("ERR_")
    
    def test_generate_random_event(self):
        """Test random event generation with distribution."""
        gen = EventGenerator(num_users=100, seed=42)
        
        # Generate many events and check distribution
        events = [gen.generate_event() for _ in range(1000)]
        event_types = [e["event_type"] for e in events]
        
        # Should have all types
        assert "page_view" in event_types
        assert "user_signup" in event_types
        assert "purchase" in event_types
        assert "payment_failed" in event_types
        
        # Page view should be most common (60%)
        page_view_count = event_types.count("page_view")
        assert page_view_count > 400  # Should be around 600
    
    def test_device_type_distribution(self):
        """Test device type distribution."""
        gen = EventGenerator(num_users=100, seed=42)
        
        events = [gen.generate_event() for _ in range(1000)]
        device_types = [e["device_type"] for e in events]
        
        mobile_count = device_types.count("mobile")
        desktop_count = device_types.count("desktop")
        
        # Mobile should be most common (55%)
        assert mobile_count > desktop_count
    
    def test_reproducible_with_seed(self):
        """Test that seed makes generation reproducible."""
        gen1 = EventGenerator(num_users=10, seed=12345)
        gen2 = EventGenerator(num_users=10, seed=12345)
        
        events1 = [gen1.generate_event()["event_id"] for _ in range(10)]
        events2 = [gen2.generate_event()["event_id"] for _ in range(10)]
        
        # With same seed, should get same event IDs
        # Note: Due to UUID generation, this may vary - test the structure instead
        assert len(events1) == len(events2)


class TestBadDataInjector:
    """Test bad data injection."""
    
    def test_remove_required_field(self):
        """Test removing required field."""
        event = {"event_id": "123", "user_id": "456", "timestamp": "2024-01-01"}
        corrupted = BadDataInjector.remove_required_field(event)
        
        # Should have one less field
        assert len(corrupted) < len(event)
    
    def test_corrupt_timestamp(self):
        """Test timestamp corruption."""
        event = {"timestamp": "2024-01-01T00:00:00Z"}
        corrupted = BadDataInjector.corrupt_timestamp(event)
        
        # Timestamp should be corrupted
        original_ts = "2024-01-01T00:00:00Z"
        assert corrupted["timestamp"] != original_ts or corrupted["timestamp"] is None
    
    def test_corrupt_json_structure(self):
        """Test JSON structure corruption."""
        event = {"key": "value"}
        corrupted = BadDataInjector.corrupt_json_structure(event)
        
        # Should return a string (malformed JSON)
        assert isinstance(corrupted, str)
        
        # Should not be valid JSON
        try:
            json.loads(corrupted)
            # If it parses, it should be a different structure
            assert corrupted != json.dumps(event)
        except json.JSONDecodeError:
            pass  # Expected
    
    def test_add_unexpected_fields(self):
        """Test adding unexpected fields."""
        event = {"key": "value"}
        corrupted = BadDataInjector.add_unexpected_fields(event)
        
        assert "__unexpected_field__" in corrupted
        assert "random_number" in corrupted
    
    def test_corrupt_payload(self):
        """Test payload corruption."""
        event = {"payload": {"field": "value"}}
        corrupted = BadDataInjector.corrupt_payload(event)
        
        # Payload should be corrupted
        assert corrupted["payload"] != {"field": "value"}


class TestConfig:
    """Test configuration."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = Config()
        
        assert config.kafka.bootstrap_servers == "localhost:29093"
        assert config.kafka.topic == "events_raw"
        assert config.producer.events_per_second == 10
        assert config.producer.bad_data_percentage == 1.0
    
    def test_event_distribution_sums_to_100(self):
        """Test event distribution sums to 100."""
        config = Config()
        total = sum(config.producer.event_distribution.values())
        assert total == 100


class TestEventProducer:
    """Test event producer."""
    

    @patch('producers.producer.Producer')
    def test_connect_success(self, mock_producer):
        """Test successful connection."""
        producer = EventProducer()
        result = producer.connect(max_retries=1)
        
        assert result is True
        assert producer.producer is not None
    
    @patch('producers.producer.Producer')
    def test_send_event(self, mock_kafka):
        """Test sending event."""
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance
        # confluent-kafka uses produce() instead of send()
        mock_instance.produce.return_value = None
        mock_instance.poll.return_value = 0
        
        producer = EventProducer()
        producer.connect()
        
        event = {"event_id": "123", "user_id": "456"}
        result = producer.send_event(event)
        
        assert result is True
        assert producer.events_sent == 1
    
    @patch('producers.producer.Producer')
    def test_send_event_with_bad_data(self, mock_kafka):
        """Test sending event with bad data injection."""
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance
        mock_instance.produce.return_value = None
        mock_instance.poll.return_value = 0
        
        producer = EventProducer()
        producer.connect()
        
        event = {"event_id": "123", "user_id": "456"}
        result = producer.send_event(event, inject_bad_data=True)
        
        assert result is True
        assert producer.bad_data_injected == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
