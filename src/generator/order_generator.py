#!/usr/bin/env python3
"""
Synthetic E-Commerce Order Generator
====================================
Generates realistic, validated e-commerce order events and publishes them
to RabbitMQ with durable queues, direct exchange routing, and publisher confirmations.

Part of Mini Project 01: E-Commerce Data Engineering Pipeline (Phase 2).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pika
from pika.exceptions import AMQPChannelError, AMQPConnectionError, UnroutableError
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# Configuration & Environment Loading
# ------------------------------------------------------------------------------

# Locate project root (two levels up from src/generator/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

if ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE)
else:
    load_dotenv()  # Fallback to default search

# Configure UTF-8 encoding on standard streams to avoid Windows charmap errors
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

SCHEMA_VERSION = "1.0.0"
EVENT_TYPE = "order.created"
CURRENCY = "INR"

# ------------------------------------------------------------------------------
# Synthetic Seed Data (Indian Market Demographics & Product Catalog)
# ------------------------------------------------------------------------------

FIRST_NAMES = [
    "Aarav", "Vihaan", "Vivaan", "Ananya", "Diya", "Advik", "Kabir", "Ishaan",
    "Sai", "Aadhya", "Pari", "Saanvi", "Riya", "Sneha", "Rohan", "Priya",
    "Arjun", "Aditya", "Meera", "Neha", "Rahul", "Kavya", "Tanvi", "Siddharth",
]

LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Iyer", "Rao", "Reddy", "Nair", "Gupta",
    "Singh", "Chatterjee", "Kulkarni", "Joshi", "Deshmukh", "Menon", "Pillai",
    "Mehta", "Bose", "Chopra", "Das", "Agarwal",
]

# (City, State, Postal Code Prefix)
LOCATIONS: List[Tuple[str, str, str]] = [
    ("Mumbai", "Maharashtra", "400001"),
    ("Pune", "Maharashtra", "411001"),
    ("Bengaluru", "Karnataka", "560001"),
    ("Mysuru", "Karnataka", "570001"),
    ("Hyderabad", "Telangana", "500001"),
    ("Chennai", "Tamil Nadu", "600001"),
    ("Coimbatore", "Tamil Nadu", "641001"),
    ("Delhi", "Delhi", "110001"),
    ("Kolkata", "West Bengal", "700001"),
    ("Ahmedabad", "Gujarat", "380001"),
    ("Jaipur", "Rajasthan", "302001"),
    ("Lucknow", "Uttar Pradesh", "226001"),
    ("Chandigarh", "Chandigarh", "160017"),
    ("Kochi", "Kerala", "682001"),
]

STREETS = [
    "MG Road", "Brigade Road", "Park Street", "Connaught Place", "Bandra West",
    "Indiranagar", "Jubilee Hills", "Anna Nagar", "Civil Lines", "FC Road",
    "Lavelle Road", "Koramangala 4th Block", "Gariahat Road", "Banjara Hills",
]

PRODUCT_CATALOG: List[Dict[str, Any]] = [
    # Electronics
    {"product_id": "PROD_ELEC_01", "name": "Noise Cancelling Wireless Headphones", "category": "Electronics", "unit_price": 4999.00},
    {"product_id": "PROD_ELEC_02", "name": "Smart Fitness Band with SpO2", "category": "Electronics", "unit_price": 2499.00},
    {"product_id": "PROD_ELEC_03", "name": "Fast Charging Power Bank 20000mAh", "category": "Electronics", "unit_price": 1899.00},
    {"product_id": "PROD_ELEC_04", "name": "Bluetooth Portable Speaker 16W", "category": "Electronics", "unit_price": 1599.00},
    {"product_id": "PROD_ELEC_05", "name": "Ergonomic Optical Wireless Mouse", "category": "Electronics", "unit_price": 799.00},
    {"product_id": "PROD_ELEC_06", "name": "Mechanical Gaming Keyboard RGB", "category": "Electronics", "unit_price": 3499.00},
    {"product_id": "PROD_ELEC_07", "name": "Multi-Port USB-C Hub 7-in-1", "category": "Electronics", "unit_price": 2199.00},

    # Home & Kitchen
    {"product_id": "PROD_HOME_01", "name": "Stainless Steel Insulated Bottle 1L", "category": "Home & Kitchen", "unit_price": 699.00},
    {"product_id": "PROD_HOME_02", "name": "Ceramic Non-Stick Frying Pan 24cm", "category": "Home & Kitchen", "unit_price": 1299.00},
    {"product_id": "PROD_HOME_03", "name": "Automatic Electric Kettle 1.5L", "category": "Home & Kitchen", "unit_price": 999.00},
    {"product_id": "PROD_HOME_04", "name": "Aromatherapy Essential Oil Diffuser", "category": "Home & Kitchen", "unit_price": 1499.00},
    {"product_id": "PROD_HOME_05", "name": "Microfiber Quick-Dry Bath Towel Set", "category": "Home & Kitchen", "unit_price": 899.00},
    {"product_id": "PROD_HOME_06", "name": "Airtight Glass Food Container Set", "category": "Home & Kitchen", "unit_price": 1199.00},

    # Fashion
    {"product_id": "PROD_FASH_01", "name": "Pure Cotton Slim Fit Casual Shirt", "category": "Fashion", "unit_price": 1199.00},
    {"product_id": "PROD_FASH_02", "name": "Comfort Stretch Denim Jeans", "category": "Fashion", "unit_price": 1799.00},
    {"product_id": "PROD_FASH_03", "name": "Lightweight Running Sneakers", "category": "Fashion", "unit_price": 2499.00},
    {"product_id": "PROD_FASH_04", "name": "Handcrafted Genuine Leather Wallet", "category": "Fashion", "unit_price": 849.00},
    {"product_id": "PROD_FASH_05", "name": "UV Protection Polarized Sunglasses", "category": "Fashion", "unit_price": 1299.00},
    {"product_id": "PROD_FASH_06", "name": "Water Resistant Canvas Backpack 25L", "category": "Fashion", "unit_price": 1699.00},

    # Stationery
    {"product_id": "PROD_STAT_01", "name": "Hardcover Ruled Notebook Journal A5", "category": "Stationery", "unit_price": 349.00},
    {"product_id": "PROD_STAT_02", "name": "Executive Metal Rollerball Pen Set", "category": "Stationery", "unit_price": 599.00},
    {"product_id": "PROD_STAT_03", "name": "Desk Organizer with Sticky Notes", "category": "Stationery", "unit_price": 499.00},
    {"product_id": "PROD_STAT_04", "name": "Fine Tip Pastel Highlighters Pack 6", "category": "Stationery", "unit_price": 299.00},
    {"product_id": "PROD_STAT_05", "name": "Self-Healing Cutting Mat A3", "category": "Stationery", "unit_price": 649.00},
]

PAYMENT_METHODS = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING", "COD"]
ORDER_STATUSES = ["PLACED", "CONFIRMED", "PROCESSING"]


# ------------------------------------------------------------------------------
# Event Generation Logic
# ------------------------------------------------------------------------------

def generate_order_event() -> Dict[str, Any]:
    """
    Constructs a fully populated, mathematically consistent synthetic e-commerce order event.
    Returns:
        Dict adhering to the Phase 2 contract containing customer, order, and items.
    """
    now_utc = datetime.now(timezone.utc)
    timestamp_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    event_id = f"evt_{uuid.uuid4().hex}"
    customer_id = f"cust_{uuid.uuid4().hex[:12]}"
    order_id = f"ord_{uuid.uuid4().hex[:12]}"

    # Customer Attributes
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    city, state, postal_code = random.choice(LOCATIONS)
    email = f"{first_name.lower()}.{last_name.lower()}.{uuid.uuid4().hex[:6]}@example.in"
    phone = f"+91-{random.randint(60000, 99999)}-{random.randint(10000, 99999)}"
    street = random.choice(STREETS)
    house_no = random.randint(1, 499)
    shipping_address = f"{house_no}, {street}, {city}, {state} - {postal_code}, India"

    customer = {
        "customer_id": customer_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone": phone,
        "city": city,
        "state": state,
        "country": "India",
        "postal_code": postal_code,
        "created_at": timestamp_iso,
    }

    # Items Selection (1 to 4 distinct products)
    num_items = random.randint(1, 4)
    selected_products = random.sample(PRODUCT_CATALOG, num_items)
    items: List[Dict[str, Any]] = []
    order_total = 0.0

    for prod in selected_products:
        item_id = f"item_{uuid.uuid4().hex[:12]}"
        qty = random.randint(1, 3)
        unit_price = float(prod["unit_price"])
        item_total = round(qty * unit_price, 2)
        order_total = round(order_total + item_total, 2)

        items.append({
            "item_id": item_id,
            "order_id": order_id,
            "product_id": prod["product_id"],
            "product_name": prod["name"],
            "category": prod["category"],
            "quantity": qty,
            "unit_price": unit_price,
            "total_price": item_total,
            "created_at": timestamp_iso,
        })

    # Order Header Attributes
    order = {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_status": random.choice(ORDER_STATUSES),
        "order_date": timestamp_iso,
        "total_amount": order_total,
        "currency": CURRENCY,
        "payment_method": random.choice(PAYMENT_METHODS),
        "shipping_address": shipping_address,
    }

    # Validation Guard
    calculated_sum = round(sum(i["total_price"] for i in items), 2)
    assert abs(order_total - calculated_sum) < 0.001, "Order total mismatch with line items sum"
    assert len(items) >= 1, "Order must contain at least 1 item"

    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": EVENT_TYPE,
        "event_id": event_id,
        "event_timestamp": timestamp_iso,
        "customer": customer,
        "order": order,
        "items": items,
    }


# ------------------------------------------------------------------------------
# RabbitMQ Publishing Client
# ------------------------------------------------------------------------------

class RabbitMQOrderPublisher:
    """Manages connection, topology declaration, and resilient publishing to RabbitMQ."""

    def __init__(self) -> None:
        # Load parameters with environment variable fallbacks
        self.host = os.getenv("GENERATOR_RABBITMQ_HOST", os.getenv("RABBITMQ_HOST", "localhost"))
        self.port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self.user = os.getenv("RABBITMQ_USER", "ecommerce_app")
        self.password = os.getenv("RABBITMQ_PASSWORD", "")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "orders_exchange")
        self.queue = os.getenv("RABBITMQ_QUEUE", "ecommerce_orders")
        self.routing_key = os.getenv("RABBITMQ_ROUTING_KEY", "order.created")

        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel | None = None

    def connect(self) -> None:
        """Establishes connection, configures channel, and declares topology."""
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )

        try:
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # Enable Publisher Confirmations
            self.channel.confirm_delivery()

            # Declare durable direct exchange
            self.channel.exchange_declare(
                exchange=self.exchange,
                exchange_type="direct",
                durable=True,
            )

            # Declare durable queue
            self.channel.queue_declare(
                queue=self.queue,
                durable=True,
            )

            # Bind queue to exchange using the routing key
            self.channel.queue_bind(
                queue=self.queue,
                exchange=self.exchange,
                routing_key=self.routing_key,
            )

        except AMQPConnectionError as err:
            raise RuntimeError(
                f"Failed to connect to RabbitMQ broker at {self.host}:{self.port} "
                f"with user '{self.user}'. Ensure the service is healthy."
            ) from err

    def publish_order(self, event: Dict[str, Any]) -> bool:
        """
        Publishes a single order event with persistent delivery and publisher confirmation.
        """
        if not self.channel or not self.channel.is_open:
            raise RuntimeError("RabbitMQ channel is not open. Call connect() first.")

        payload_bytes = json.dumps(event, ensure_ascii=False).encode("utf-8")

        properties = pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=event["event_id"],
            type=event["event_type"],
            timestamp=int(datetime.now(timezone.utc).timestamp()),
            headers={
                "schema_version": event["schema_version"],
                "order_id": event["order"]["order_id"],
                "customer_id": event["customer"]["customer_id"],
            },
        )

        try:
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.routing_key,
                body=payload_bytes,
                properties=properties,
                mandatory=True,
            )
            return True
        except (AMQPChannelError, UnroutableError) as err:
            raise RuntimeError(f"RabbitMQ message publication rejected: {err}") from err

    def close(self) -> None:
        """Gracefully closes channel and connection."""
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
        except Exception:
            pass
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass


# ------------------------------------------------------------------------------
# CLI Runner
# ------------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Synthetic E-Commerce Order Generator for RabbitMQ",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of synthetic orders to publish",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Delay in seconds between successive orders",
    )

    args = parser.parse_args()

    # Argument validation
    if args.count <= 0:
        print(f"Error: --count must be a positive integer greater than 0. Received: {args.count}", file=sys.stderr)
        return 1

    if args.interval < 0:
        print(f"Error: --interval cannot be negative. Received: {args.interval}", file=sys.stderr)
        return 1

    publisher = RabbitMQOrderPublisher()

    print("=" * 80)
    print("  E-COMMERCE SYNTHETIC ORDER GENERATOR (PHASE 2)")
    print("=" * 80)
    print(f"  Target Broker : {publisher.host}:{publisher.port}")
    print(f"  User          : {publisher.user}")
    print(f"  Exchange      : {publisher.exchange} (type: direct, durable: true)")
    print(f"  Routing Key   : {publisher.routing_key}")
    print(f"  Queue         : {publisher.queue} (durable: true)")
    print(f"  Order Count   : {args.count}")
    print(f"  Interval      : {args.interval}s")
    print("-" * 80)

    try:
        publisher.connect()
        print("[OK] Connected to RabbitMQ and verified exchange/queue bindings.\n")
    except Exception as err:
        print(f"[ERROR] Connection Error: {err}", file=sys.stderr)
        return 1

    published_count = 0
    start_time = time.time()

    try:
        for idx in range(1, args.count + 1):
            event = generate_order_event()
            order = event["order"]
            customer = event["customer"]
            item_count = len(event["items"])

            publisher.publish_order(event)
            published_count += 1

            print(
                f"  [{idx}/{args.count}] [OK] Published {order['order_id']} | "
                f"Customer: {customer['first_name']} {customer['last_name']} | "
                f"Items: {item_count} | Total: INR {order['total_amount']:,.2f} "
                f"-> [{publisher.routing_key}]"
            )

            if idx < args.count and args.interval > 0:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[!] Publishing interrupted by user.")
    except Exception as err:
        print(f"\n[ERROR] Error during order generation: {err}", file=sys.stderr)
        return 1
    finally:
        publisher.close()

    elapsed = round(time.time() - start_time, 2)
    print("\n" + "=" * 80)
    print(
        f"  SUMMARY: Successfully published {published_count}/{args.count} orders "
        f"in {elapsed}s to queue '{publisher.queue}'."
    )
    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
