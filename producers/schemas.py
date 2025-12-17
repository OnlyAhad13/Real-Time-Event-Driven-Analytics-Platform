"""
Real-Time Analytics Platform - Event Schemas
============================================
Defines event schemas for all supported event types with schema versioning.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List
from enum import Enum
import uuid


class SchemaVersion(str, Enum):
    """Supported schema versions."""
    V1 = "v1"


class EventType(str, Enum):
    """Supported event types."""
    USER_SIGNUP = "user_signup"
    PAGE_VIEW = "page_view"
    PURCHASE = "purchase"
    PAYMENT_FAILED = "payment_failed"


class DeviceType(str, Enum):
    """Device types for event tracking."""
    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"
    OTHER = "other"


@dataclass
class BaseEvent:
    """
    Base event schema with common fields.
    All events inherit from this base class.
    """
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    event_type: str = ""
    device_type: str = DeviceType.DESKTOP.value
    ip_address: str = ""
    schema_version: str = SchemaVersion.V1.value
    idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return asdict(self)


@dataclass
class UserSignupPayload:
    """Payload specific to user signup events."""
    email: str = ""
    username: str = ""
    signup_source: str = ""  # organic, referral, paid_ad, social
    referral_code: Optional[str] = None
    accepted_terms: bool = True
    marketing_consent: bool = False


@dataclass
class PageViewPayload:
    """Payload specific to page view events."""
    page_url: str = ""
    page_title: str = ""
    referrer_url: Optional[str] = None
    session_id: str = ""
    duration_seconds: Optional[int] = None
    scroll_depth_percent: Optional[int] = None


@dataclass
class PurchasePayload:
    """Payload specific to purchase events."""
    transaction_id: str = ""
    product_id: str = ""
    product_name: str = ""
    category: str = ""
    quantity: int = 1
    unit_price: float = 0.0
    total_amount: float = 0.0
    currency: str = "USD"
    payment_method: str = ""  # credit_card, debit_card, paypal, crypto
    discount_code: Optional[str] = None
    discount_amount: float = 0.0


@dataclass
class PaymentFailedPayload:
    """Payload specific to payment failed events."""
    transaction_id: str = ""
    product_id: str = ""
    attempted_amount: float = 0.0
    currency: str = "USD"
    payment_method: str = ""
    failure_reason: str = ""  # insufficient_funds, card_declined, expired_card, fraud_detected
    error_code: str = ""
    retry_count: int = 0


@dataclass
class UserSignupEvent(BaseEvent):
    """Complete user signup event."""
    event_type: str = EventType.USER_SIGNUP.value
    payload: UserSignupPayload = field(default_factory=UserSignupPayload)
    
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data['payload'] = asdict(self.payload)
        return data


@dataclass
class PageViewEvent(BaseEvent):
    """Complete page view event."""
    event_type: str = EventType.PAGE_VIEW.value
    payload: PageViewPayload = field(default_factory=PageViewPayload)
    
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data['payload'] = asdict(self.payload)
        return data


@dataclass
class PurchaseEvent(BaseEvent):
    """Complete purchase event."""
    event_type: str = EventType.PURCHASE.value
    payload: PurchasePayload = field(default_factory=PurchasePayload)
    
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data['payload'] = asdict(self.payload)
        return data


@dataclass
class PaymentFailedEvent(BaseEvent):
    """Complete payment failed event."""
    event_type: str = EventType.PAYMENT_FAILED.value
    payload: PaymentFailedPayload = field(default_factory=PaymentFailedPayload)
    
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data['payload'] = asdict(self.payload)
        return data


# Schema registry for validation (used later for schema evolution)
SCHEMA_REGISTRY = {
    SchemaVersion.V1.value: {
        EventType.USER_SIGNUP.value: {
            "required_fields": ["event_id", "user_id", "timestamp", "event_type", "schema_version", "idempotency_key"],
            "payload_fields": ["email", "username", "signup_source"],
        },
        EventType.PAGE_VIEW.value: {
            "required_fields": ["event_id", "user_id", "timestamp", "event_type", "schema_version", "idempotency_key"],
            "payload_fields": ["page_url", "session_id"],
        },
        EventType.PURCHASE.value: {
            "required_fields": ["event_id", "user_id", "timestamp", "event_type", "schema_version", "idempotency_key"],
            "payload_fields": ["transaction_id", "product_id", "total_amount", "currency"],
        },
        EventType.PAYMENT_FAILED.value: {
            "required_fields": ["event_id", "user_id", "timestamp", "event_type", "schema_version", "idempotency_key"],
            "payload_fields": ["transaction_id", "failure_reason", "error_code"],
        },
    }
}
