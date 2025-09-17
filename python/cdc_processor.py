"""
CDC (Change Data Capture) Stream Processor
Consumes change events from Debezium via Kafka and processes them
"""
import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
from typing import Dict, Any
from utils import KafkaManager, RedisManager, setup_logging
from config import get_config

class CDCProcessor:
    """Process CDC events from Kafka topics"""
    
    def __init__(self, kafka_manager: KafkaManager, redis_manager: RedisManager, logger: logging.Logger):
        self.kafka = kafka_manager
        self.redis = redis_manager
        self.logger = logger
        self.processed_count = 0
    
    def process_change_event(self, message: Dict[str, Any]) -> bool:
        """Process a single CDC change event"""
        try:
            payload = message.get('payload', {})
            operation = payload.get('op')  # c=create, u=update, d=delete, r=read
            source = payload.get('source', {})
            table = source.get('table')
            timestamp = payload.get('ts_ms', int(time.time() * 1000))
            
            # Extract before and after data
            before_data = payload.get('before')
            after_data = payload.get('after')
            
            change_event = {
                'operation': operation,
                'table': table,
                'timestamp': timestamp,
                'before': before_data,
                'after': after_data,
                'processed_at': datetime.now().isoformat()
            }
            
            # Store change event in Redis for further processing
            redis_key = f"cdc:change:{table}:{timestamp}:{operation}"
            self.redis.set_data(redis_key, change_event, expiry=3600)  # 1 hour expiry
            
            # Update statistics
            self.redis.increment_counter(f"cdc:stats:{table}:{operation}")
            self.redis.increment_counter("cdc:stats:total")
            
            # Process specific table changes
            if table == 'customers':
                self.process_customer_change(change_event)
            elif table == 'orders':
                self.process_order_change(change_event)
            elif table == 'products':
                self.process_product_change(change_event)
            elif table == 'order_items':
                self.process_order_item_change(change_event)
            
            self.processed_count += 1
            self.logger.debug(f"Processed {operation} on {table}, total processed: {self.processed_count}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing change event: {e}")
            return False
    
    def process_customer_change(self, change_event: Dict[str, Any]):
        """Process customer table changes"""
        operation = change_event['operation']
        after_data = change_event.get('after')
        
        if operation in ['c', 'u'] and after_data:  # Create or Update
            customer_id = after_data.get('id')
            
            # Cache customer data for quick lookup
            customer_cache_key = f"customer:{customer_id}"
            self.redis.set_data(customer_cache_key, after_data, expiry=7200)  # 2 hours
            
            # Update customer metrics
            if operation == 'c':
                self.redis.increment_counter("metrics:customers:created")
                self.logger.info(f"New customer created: {customer_id}")
            else:
                self.redis.increment_counter("metrics:customers:updated")
                self.logger.info(f"Customer updated: {customer_id}")
        
        elif operation == 'd':  # Delete
            before_data = change_event.get('before')
            if before_data:
                customer_id = before_data.get('id')
                # Remove from cache
                redis_client = self.redis.get_client()
                redis_client.delete(f"customer:{customer_id}")
                self.redis.increment_counter("metrics:customers:deleted")
                self.logger.info(f"Customer deleted: {customer_id}")
    
    def process_order_change(self, change_event: Dict[str, Any]):
        """Process order table changes"""
        operation = change_event['operation']
        after_data = change_event.get('after')
        
        if operation in ['c', 'u'] and after_data:
            order_id = after_data.get('id')
            customer_id = after_data.get('customer_id')
            total_amount = after_data.get('total_amount')
            
            # Cache order data
            order_cache_key = f"order:{order_id}"
            self.redis.set_data(order_cache_key, after_data, expiry=3600)
            
            # Update customer order statistics
            if customer_id:
                customer_orders_key = f"customer:{customer_id}:orders"
                redis_client = self.redis.get_client()
                redis_client.sadd(customer_orders_key, order_id)
                redis_client.expire(customer_orders_key, 7200)
            
            # Update revenue metrics
            if operation == 'c' and total_amount:
                redis_client = self.redis.get_client()
                redis_client.incrbyfloat("metrics:revenue:total", float(total_amount))
                self.redis.increment_counter("metrics:orders:created")
                self.logger.info(f"New order created: {order_id}, amount: ${total_amount}")
            
            elif operation == 'u':
                self.redis.increment_counter("metrics:orders:updated")
                self.logger.info(f"Order updated: {order_id}")
        
        elif operation == 'd':
            before_data = change_event.get('before')
            if before_data:
                order_id = before_data.get('id')
                customer_id = before_data.get('customer_id')
                
                # Remove from cache
                redis_client = self.redis.get_client()
                redis_client.delete(f"order:{order_id}")
                
                if customer_id:
                    redis_client.srem(f"customer:{customer_id}:orders", order_id)
                
                self.redis.increment_counter("metrics:orders:deleted")
                self.logger.info(f"Order deleted: {order_id}")
    
    def process_product_change(self, change_event: Dict[str, Any]):
        """Process product table changes"""
        operation = change_event['operation']
        after_data = change_event.get('after')
        
        if operation in ['c', 'u'] and after_data:
            product_id = after_data.get('id')
            stock_quantity = after_data.get('stock_quantity')
            
            # Cache product data
            product_cache_key = f"product:{product_id}"
            self.redis.set_data(product_cache_key, after_data, expiry=3600)
            
            # Track low stock alerts
            if stock_quantity is not None and stock_quantity < 10:
                alert_key = f"alert:low_stock:{product_id}"
                alert_data = {
                    'product_id': product_id,
                    'current_stock': stock_quantity,
                    'alert_time': datetime.now().isoformat()
                }
                self.redis.set_data(alert_key, alert_data, expiry=1800)  # 30 minutes
                self.logger.warning(f"Low stock alert for product {product_id}: {stock_quantity} units")
            
            if operation == 'c':
                self.redis.increment_counter("metrics:products:created")
                self.logger.info(f"New product created: {product_id}")
            else:
                self.redis.increment_counter("metrics:products:updated")
                self.logger.info(f"Product updated: {product_id}")
        
        elif operation == 'd':
            before_data = change_event.get('before')
            if before_data:
                product_id = before_data.get('id')
                # Remove from cache
                redis_client = self.redis.get_client()
                redis_client.delete(f"product:{product_id}")
                redis_client.delete(f"alert:low_stock:{product_id}")
                self.redis.increment_counter("metrics:products:deleted")
                self.logger.info(f"Product deleted: {product_id}")
    
    def process_order_item_change(self, change_event: Dict[str, Any]):
        """Process order_items table changes"""
        operation = change_event['operation']
        after_data = change_event.get('after')
        
        if operation in ['c', 'u'] and after_data:
            order_id = after_data.get('order_id')
            product_id = after_data.get('product_id')
            quantity = after_data.get('quantity')
            
            # Update product sales metrics
            if product_id and quantity:
                redis_client = self.redis.get_client()
                redis_client.incr(f"metrics:product:{product_id}:sold", quantity)
            
            if operation == 'c':
                self.redis.increment_counter("metrics:order_items:created")
                self.logger.debug(f"Order item created for order {order_id}, product {product_id}")
        
        elif operation == 'd':
            self.redis.increment_counter("metrics:order_items:deleted")
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        redis_client = self.redis.get_client()
        
        stats = {
            'processed_count': self.processed_count,
            'total_changes': redis_client.get("cdc:stats:total") or 0,
            'customers': {
                'created': redis_client.get("metrics:customers:created") or 0,
                'updated': redis_client.get("metrics:customers:updated") or 0,
                'deleted': redis_client.get("metrics:customers:deleted") or 0
            },
            'orders': {
                'created': redis_client.get("metrics:orders:created") or 0,
                'updated': redis_client.get("metrics:orders:updated") or 0,
                'deleted': redis_client.get("metrics:orders:deleted") or 0
            },
            'products': {
                'created': redis_client.get("metrics:products:created") or 0,
                'updated': redis_client.get("metrics:products:updated") or 0,
                'deleted': redis_client.get("metrics:products:deleted") or 0
            },
            'total_revenue': float(redis_client.get("metrics:revenue:total") or 0)
        }
        
        return stats

def main():
    """Main CDC processor loop"""
    logger = setup_logging()
    db_config, kafka_config, redis_config, app_config = get_config()
    
    logger.info("Starting CDC processor...")
    
    # Setup managers
    kafka_manager = KafkaManager(kafka_config)
    redis_manager = RedisManager(redis_config)
    
    processor = CDCProcessor(kafka_manager, redis_manager, logger)
    
    # Topics to consume from (Debezium topics)
    topics = list(kafka_config.topics.values())
    
    logger.info(f"Subscribing to topics: {topics}")
    
    try:
        # Create consumer
        consumer = kafka_manager.get_consumer(topics, group_id='cdc-processor-group')
        
        logger.info("CDC processor ready, waiting for messages...")
        
        message_count = 0
        for message in consumer:
            try:
                if message.value:
                    processor.process_change_event(message.value)
                    message_count += 1
                    
                    # Log stats every 100 messages
                    if message_count % 100 == 0:
                        stats = processor.get_processing_stats()
                        logger.info(f"Processed {message_count} messages. Stats: {stats}")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("CDC processor stopped by user")
    except Exception as e:
        logger.error(f"CDC processor error: {e}")
    finally:
        kafka_manager.close()
        redis_manager.close()
        logger.info("CDC processor shut down")

if __name__ == "__main__":
    main()