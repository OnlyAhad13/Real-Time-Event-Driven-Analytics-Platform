"""
Real-Time Analytics Platform - Event Producers
==============================================
"""

from .producer import EventProducer, main
from .generator import EventGenerator
from .schemas import (
    EventType,
    SchemaVersion,
    UserSignupEvent,
    PageViewEvent,
    PurchaseEvent,
    PaymentFailedEvent,
)
from .config import Config, load_config

__all__ = [
    "EventProducer",
    "EventGenerator",
    "EventType",
    "SchemaVersion",
    "UserSignupEvent",
    "PageViewEvent",
    "PurchaseEvent",
    "PaymentFailedEvent",
    "Config",
    "load_config",
    "main",
]
