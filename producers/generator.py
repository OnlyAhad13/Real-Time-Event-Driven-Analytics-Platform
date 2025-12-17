"""
Real-Time Analytics Platform - Event Generator
==============================================
Generates realistic fake events using Faker library.
"""

import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from faker import Faker

from .schemas import (
    EventType,
    DeviceType,
    UserSignupEvent,
    UserSignupPayload,
    PageViewEvent,
    PageViewPayload,
    PurchaseEvent,
    PurchasePayload,
    PaymentFailedEvent,
    PaymentFailedPayload,
)


class EventGenerator:
    """
    Generates realistic fake business events for testing.
    Supports: user_signup, page_view, purchase, payment_failed
    """
    
    def __init__(self, num_users: int = 1000, seed: Optional[int] = None):
        """
        Initialize the event generator.
        
        Args:
            num_users: Number of simulated users to generate events for
            seed: Random seed for reproducibility (optional)
        """
        self.fake = Faker()
        if seed:
            Faker.seed(seed)
            random.seed(seed)
        
        # Pre-generate user pool for realistic user_id reuse
        self.user_pool = [str(uuid.uuid4()) for _ in range(num_users)]
        
        # Session pool for page view sessionization
        self.active_sessions: Dict[str, str] = {}
        
        # Product catalog for purchases
        self.products = self._generate_product_catalog()
        
        # Page catalog for page views
        self.pages = self._generate_page_catalog()
        
        # Signup sources
        self.signup_sources = ["organic", "referral", "paid_ad", "social", "email_campaign"]
        
        # Payment methods
        self.payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
        
        # Failure reasons
        self.failure_reasons = [
            ("insufficient_funds", "ERR_001"),
            ("card_declined", "ERR_002"),
            ("expired_card", "ERR_003"),
            ("fraud_detected", "ERR_004"),
            ("invalid_cvv", "ERR_005"),
            ("network_error", "ERR_006"),
            ("bank_timeout", "ERR_007"),
        ]
        
        # Device distribution (weighted)
        self.device_weights = {
            DeviceType.MOBILE.value: 55,
            DeviceType.DESKTOP.value: 35,
            DeviceType.TABLET.value: 8,
            DeviceType.OTHER.value: 2,
        }
    
    def _generate_product_catalog(self) -> list:
        """Generate a realistic product catalog."""
        categories = {
            "Electronics": ["Smartphone", "Laptop", "Headphones", "Tablet", "Smartwatch", "Camera"],
            "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Hoodie"],
            "Home": ["Lamp", "Chair", "Desk", "Rug", "Plant", "Mirror"],
            "Books": ["Novel", "Textbook", "Cookbook", "Biography", "Comics", "Magazine"],
            "Sports": ["Running Shoes", "Yoga Mat", "Dumbbells", "Bicycle", "Tennis Racket"],
        }
        
        products = []
        for category, items in categories.items():
            for item in items:
                products.append({
                    "product_id": f"PROD-{uuid.uuid4().hex[:8].upper()}",
                    "product_name": f"{self.fake.company()} {item}",
                    "category": category,
                    "price": round(random.uniform(9.99, 999.99), 2),
                })
        return products
    
    def _generate_page_catalog(self) -> list:
        """Generate a realistic page catalog."""
        return [
            {"url": "/", "title": "Home"},
            {"url": "/products", "title": "Products"},
            {"url": "/products/category/electronics", "title": "Electronics"},
            {"url": "/products/category/clothing", "title": "Clothing"},
            {"url": "/products/category/home", "title": "Home & Garden"},
            {"url": "/cart", "title": "Shopping Cart"},
            {"url": "/checkout", "title": "Checkout"},
            {"url": "/account", "title": "My Account"},
            {"url": "/account/orders", "title": "Order History"},
            {"url": "/account/settings", "title": "Account Settings"},
            {"url": "/search", "title": "Search Results"},
            {"url": "/about", "title": "About Us"},
            {"url": "/contact", "title": "Contact"},
            {"url": "/blog", "title": "Blog"},
            {"url": "/help", "title": "Help Center"},
        ]
    
    def _get_random_user(self) -> str:
        """Get a random user from the pool."""
        return random.choice(self.user_pool)
    
    def _get_random_device(self) -> str:
        """Get a weighted random device type."""
        devices = list(self.device_weights.keys())
        weights = list(self.device_weights.values())
        return random.choices(devices, weights=weights, k=1)[0]
    
    def _get_or_create_session(self, user_id: str) -> str:
        """Get existing session or create new one for user."""
        # 70% chance to continue existing session, 30% new session
        if user_id in self.active_sessions and random.random() < 0.7:
            return self.active_sessions[user_id]
        
        session_id = str(uuid.uuid4())
        self.active_sessions[user_id] = session_id
        
        # Clean up old sessions (keep pool manageable)
        if len(self.active_sessions) > 5000:
            keys = list(self.active_sessions.keys())
            for key in keys[:1000]:
                del self.active_sessions[key]
        
        return session_id
    
    def generate_user_signup(self) -> UserSignupEvent:
        """Generate a user signup event."""
        # New user gets a new UUID (not from pool)
        new_user_id = str(uuid.uuid4())
        
        # Add to pool for future events
        if len(self.user_pool) < 10000:
            self.user_pool.append(new_user_id)
        
        payload = UserSignupPayload(
            email=self.fake.email(),
            username=self.fake.user_name(),
            signup_source=random.choice(self.signup_sources),
            referral_code=self.fake.bothify("REF-####??").upper() if random.random() < 0.3 else None,
            accepted_terms=True,
            marketing_consent=random.random() < 0.4,
        )
        
        return UserSignupEvent(
            user_id=new_user_id,
            device_type=self._get_random_device(),
            ip_address=self.fake.ipv4(),
            payload=payload,
        )
    
    def generate_page_view(self) -> PageViewEvent:
        """Generate a page view event."""
        user_id = self._get_random_user()
        page = random.choice(self.pages)
        session_id = self._get_or_create_session(user_id)
        
        # Generate realistic referrer
        referrer = None
        if random.random() < 0.6:
            referrer_options = [
                "https://www.google.com/search?q=...",
                "https://www.facebook.com/",
                "https://twitter.com/",
                "https://www.instagram.com/",
                None,  # direct traffic
            ]
            referrer = random.choice(referrer_options)
        
        payload = PageViewPayload(
            page_url=f"https://example.com{page['url']}",
            page_title=page["title"],
            referrer_url=referrer,
            session_id=session_id,
            duration_seconds=random.randint(5, 300) if random.random() < 0.8 else None,
            scroll_depth_percent=random.randint(10, 100) if random.random() < 0.7 else None,
        )
        
        return PageViewEvent(
            user_id=user_id,
            device_type=self._get_random_device(),
            ip_address=self.fake.ipv4(),
            payload=payload,
        )
    
    def generate_purchase(self) -> PurchaseEvent:
        """Generate a purchase event."""
        user_id = self._get_random_user()
        product = random.choice(self.products)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[70, 15, 8, 4, 3], k=1)[0]
        
        unit_price = product["price"]
        discount = 0.0
        discount_code = None
        
        # 20% chance of discount
        if random.random() < 0.2:
            discount_code = self.fake.bothify("SAVE##").upper()
            discount = round(unit_price * quantity * random.uniform(0.05, 0.25), 2)
        
        total = round(unit_price * quantity - discount, 2)
        
        payload = PurchasePayload(
            transaction_id=f"TXN-{uuid.uuid4().hex[:12].upper()}",
            product_id=product["product_id"],
            product_name=product["product_name"],
            category=product["category"],
            quantity=quantity,
            unit_price=unit_price,
            total_amount=total,
            currency=random.choices(["USD", "EUR", "GBP"], weights=[80, 15, 5], k=1)[0],
            payment_method=random.choice(self.payment_methods),
            discount_code=discount_code,
            discount_amount=discount,
        )
        
        return PurchaseEvent(
            user_id=user_id,
            device_type=self._get_random_device(),
            ip_address=self.fake.ipv4(),
            payload=payload,
        )
    
    def generate_payment_failed(self) -> PaymentFailedEvent:
        """Generate a payment failed event."""
        user_id = self._get_random_user()
        product = random.choice(self.products)
        failure_reason, error_code = random.choice(self.failure_reasons)
        
        payload = PaymentFailedPayload(
            transaction_id=f"TXN-{uuid.uuid4().hex[:12].upper()}",
            product_id=product["product_id"],
            attempted_amount=round(product["price"] * random.randint(1, 3), 2),
            currency=random.choices(["USD", "EUR", "GBP"], weights=[80, 15, 5], k=1)[0],
            payment_method=random.choice(self.payment_methods),
            failure_reason=failure_reason,
            error_code=error_code,
            retry_count=random.choices([0, 1, 2, 3], weights=[50, 30, 15, 5], k=1)[0],
        )
        
        return PaymentFailedEvent(
            user_id=user_id,
            device_type=self._get_random_device(),
            ip_address=self.fake.ipv4(),
            payload=payload,
        )
    
    def generate_event(self, event_type: str = None, distribution: dict = None) -> Dict[str, Any]:
        """
        Generate a random event based on distribution or specified type.
        
        Args:
            event_type: Specific event type to generate (optional)
            distribution: Event type distribution weights (optional)
        
        Returns:
            Event dictionary ready for serialization
        """
        if event_type is None:
            if distribution is None:
                distribution = {
                    "page_view": 60,
                    "user_signup": 10,
                    "purchase": 20,
                    "payment_failed": 10,
                }
            
            event_types = list(distribution.keys())
            weights = list(distribution.values())
            event_type = random.choices(event_types, weights=weights, k=1)[0]
        
        generators = {
            EventType.USER_SIGNUP.value: self.generate_user_signup,
            EventType.PAGE_VIEW.value: self.generate_page_view,
            EventType.PURCHASE.value: self.generate_purchase,
            EventType.PAYMENT_FAILED.value: self.generate_payment_failed,
        }
        
        generator = generators.get(event_type)
        if generator is None:
            raise ValueError(f"Unknown event type: {event_type}")
        
        return generator().to_dict()
