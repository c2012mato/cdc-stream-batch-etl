"""
Airflow 3.0 Asset definitions for CDC Stream Batch ETL
Using airflow.sdk for asset management and data lineage tracking
"""
from airflow.sdk.definitions.asset import Asset
from typing import Dict, Any

# Database table assets for lineage tracking
CUSTOMERS_TABLE = Asset(
    uri="postgresql://postgres@postgres:5432/source_db/public/customers",
    name="customers_table",
    metadata={
        "description": "Customer information and demographics",
        "schema": "public",
        "table": "customers",
        "type": "source_table"
    }
)

PRODUCTS_TABLE = Asset(
    uri="postgresql://postgres@postgres:5432/source_db/public/products", 
    name="products_table",
    metadata={
        "description": "Product catalog with categories and pricing",
        "schema": "public", 
        "table": "products",
        "type": "source_table"
    }
)

ORDERS_TABLE = Asset(
    uri="postgresql://postgres@postgres:5432/source_db/public/orders",
    name="orders_table", 
    metadata={
        "description": "Customer orders with status tracking",
        "schema": "public",
        "table": "orders", 
        "type": "source_table"
    }
)

ORDER_ITEMS_TABLE = Asset(
    uri="postgresql://postgres@postgres:5432/source_db/public/order_items",
    name="order_items_table",
    metadata={
        "description": "Individual items within orders", 
        "schema": "public",
        "table": "order_items",
        "type": "source_table"
    }
)

# Kafka topic assets for CDC events
CUSTOMERS_CDC_TOPIC = Asset(
    uri="kafka://kafka:29092/dbserver1.public.customers",
    name="customers_cdc_events",
    metadata={
        "description": "Customer change data capture events",
        "topic": "dbserver1.public.customers",
        "type": "cdc_stream"
    }
)

ORDERS_CDC_TOPIC = Asset(
    uri="kafka://kafka:29092/dbserver1.public.orders", 
    name="orders_cdc_events",
    metadata={
        "description": "Order change data capture events",
        "topic": "dbserver1.public.orders",
        "type": "cdc_stream"
    }
)

PRODUCTS_CDC_TOPIC = Asset(
    uri="kafka://kafka:29092/dbserver1.public.products",
    name="products_cdc_events", 
    metadata={
        "description": "Product change data capture events",
        "topic": "dbserver1.public.products",
        "type": "cdc_stream"
    }
)

ORDER_ITEMS_CDC_TOPIC = Asset(
    uri="kafka://kafka:29092/dbserver1.public.order_items",
    name="order_items_cdc_events",
    metadata={
        "description": "Order items change data capture events", 
        "topic": "dbserver1.public.order_items",
        "type": "cdc_stream"
    }
)

# Redis cache assets for processed data
CUSTOMER_SEGMENTS_CACHE = Asset(
    uri="redis://redis:6379/0/customer_segments", 
    name="customer_segments",
    metadata={
        "description": "Customer segmentation analytics (Gold, Silver, Bronze)",
        "key_pattern": "customer_segments:*",
        "type": "analytics_cache"
    }
)

PRODUCT_PERFORMANCE_CACHE = Asset(
    uri="redis://redis:6379/0/product_performance",
    name="product_performance",
    metadata={
        "description": "Product performance insights and categories",
        "key_pattern": "product_performance:*", 
        "type": "analytics_cache"
    }
)

REAL_TIME_METRICS_CACHE = Asset(
    uri="redis://redis:6379/0/real_time_metrics",
    name="real_time_metrics", 
    metadata={
        "description": "Real-time streaming analytics and metrics",
        "key_pattern": "real_time_metrics:*",
        "type": "stream_analytics"
    }
)

ANOMALY_ALERTS_CACHE = Asset(
    uri="redis://redis:6379/0/anomaly_alerts",
    name="anomaly_alerts",
    metadata={
        "description": "Real-time anomaly detection alerts",
        "key_pattern": "anomaly_alerts:*", 
        "type": "alert_cache"
    }
)

# Aggregate analytics assets
REVENUE_ANALYTICS = Asset(
    uri="redis://redis:6379/0/revenue_analytics",
    name="revenue_analytics",
    metadata={
        "description": "Revenue analytics and business metrics",
        "key_pattern": "revenue_analytics:*",
        "type": "business_analytics"
    }
)

GEOGRAPHIC_DISTRIBUTION = Asset(
    uri="redis://redis:6379/0/geographic_distribution", 
    name="geographic_distribution",
    metadata={
        "description": "Geographic distribution analysis",
        "key_pattern": "geographic_distribution:*",
        "type": "geographic_analytics"
    }
)

# Asset groups for easier management
SOURCE_TABLES = [
    CUSTOMERS_TABLE,
    PRODUCTS_TABLE, 
    ORDERS_TABLE,
    ORDER_ITEMS_TABLE
]

CDC_STREAMS = [
    CUSTOMERS_CDC_TOPIC,
    ORDERS_CDC_TOPIC,
    PRODUCTS_CDC_TOPIC,
    ORDER_ITEMS_CDC_TOPIC
]

ANALYTICS_CACHES = [
    CUSTOMER_SEGMENTS_CACHE,
    PRODUCT_PERFORMANCE_CACHE,
    REAL_TIME_METRICS_CACHE,
    ANOMALY_ALERTS_CACHE,
    REVENUE_ANALYTICS,
    GEOGRAPHIC_DISTRIBUTION
]

ALL_ASSETS = SOURCE_TABLES + CDC_STREAMS + ANALYTICS_CACHES