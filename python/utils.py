"""
Shared utilities for the ETL system
"""
import logging
import json
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer, KafkaConsumer
from typing import Dict, Any, Optional
from config import DatabaseConfig, KafkaConfig, RedisConfig

def setup_logging(level: str = "INFO") -> logging.Logger:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

class DatabaseConnection:
    """Database connection manager"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password
            )
            return self.connection
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None) -> list:
        """Execute a SELECT query and return results"""
        if not self.connection:
            self.connect()
        
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def execute_insert(self, query: str, params: tuple = None) -> int:
        """Execute an INSERT query and return the inserted ID"""
        if not self.connection:
            self.connect()
        
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            inserted_id = cursor.fetchone()[0] if cursor.description else None
            self.connection.commit()
            return inserted_id
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()

class KafkaManager:
    """Kafka producer and consumer manager"""
    
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer = None
        self.consumer = None
    
    def get_producer(self) -> KafkaProducer:
        """Get Kafka producer instance"""
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.config.broker],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None
            )
        return self.producer
    
    def get_consumer(self, topics: list, group_id: str) -> KafkaConsumer:
        """Get Kafka consumer instance"""
        return KafkaConsumer(
            *topics,
            bootstrap_servers=[self.config.broker],
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            auto_offset_reset='earliest'
        )
    
    def send_message(self, topic: str, message: Dict[Any, Any], key: str = None):
        """Send message to Kafka topic"""
        producer = self.get_producer()
        future = producer.send(topic, value=message, key=key)
        return future
    
    def close(self):
        """Close Kafka connections"""
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()

class RedisManager:
    """Redis connection manager"""
    
    def __init__(self, config: RedisConfig):
        self.config = config
        self.client = None
    
    def get_client(self) -> redis.Redis:
        """Get Redis client instance"""
        if not self.client:
            self.client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                password=self.config.password,
                db=self.config.db,
                decode_responses=True
            )
        return self.client
    
    def set_data(self, key: str, value: Any, expiry: Optional[int] = None):
        """Set data in Redis"""
        client = self.get_client()
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        if expiry:
            client.setex(key, expiry, value)
        else:
            client.set(key, value)
    
    def get_data(self, key: str) -> Any:
        """Get data from Redis"""
        client = self.get_client()
        value = client.get(key)
        if value:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return None
    
    def increment_counter(self, key: str, amount: int = 1) -> int:
        """Increment a counter in Redis"""
        client = self.get_client()
        return client.incr(key, amount)
    
    def close(self):
        """Close Redis connection"""
        if self.client:
            self.client.close()