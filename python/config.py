"""
Configuration management for the ETL system
"""
import os
from typing import Optional
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class KafkaConfig:
    broker: str
    topics: dict = None
    
    def __post_init__(self):
        if self.topics is None:
            self.topics = {
                'customers': 'dbserver1.public.customers',
                'orders': 'dbserver1.public.orders',
                'products': 'dbserver1.public.products',
                'order_items': 'dbserver1.public.order_items'
            }

@dataclass
class RedisConfig:
    host: str
    port: int
    password: str
    db: int = 0

@dataclass
class AppConfig:
    log_level: str = "INFO"
    batch_interval_seconds: int = 60
    stream_buffer_size: int = 1000
    mock_data_records_per_batch: int = 100
    mock_data_interval_seconds: int = 10

def get_config() -> tuple[DatabaseConfig, KafkaConfig, RedisConfig, AppConfig]:
    """Get configuration from environment variables"""
    
    # Database configuration
    db_config = DatabaseConfig(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        database=os.getenv('POSTGRES_DB', 'source_db'),
        username=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
    )
    
    # Kafka configuration
    kafka_config = KafkaConfig(
        broker=os.getenv('KAFKA_BROKER', 'localhost:9092')
    )
    
    # Redis configuration
    redis_config = RedisConfig(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        password=os.getenv('REDIS_PASSWORD', 'redis123')
    )
    
    # Application configuration
    app_config = AppConfig(
        log_level=os.getenv('LOG_LEVEL', 'INFO'),
        batch_interval_seconds=int(os.getenv('BATCH_INTERVAL_SECONDS', 60)),
        stream_buffer_size=int(os.getenv('STREAM_BUFFER_SIZE', 1000)),
        mock_data_records_per_batch=int(os.getenv('MOCK_DATA_RECORDS_PER_BATCH', 100)),
        mock_data_interval_seconds=int(os.getenv('MOCK_DATA_INTERVAL_SECONDS', 10))
    )
    
    return db_config, kafka_config, redis_config, app_config