"""
Real-time Stream Processor
Processes streaming data for real-time analytics and alerts
"""
import json
import time
import logging
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import Dict, Any, List
from kafka import KafkaProducer
from utils import KafkaManager, RedisManager, setup_logging
from config import get_config

class StreamProcessor:
    """Process real-time streaming data"""
    
    def __init__(self, kafka_manager: KafkaManager, redis_manager: RedisManager, 
                 logger: logging.Logger, buffer_size: int = 1000):
        self.kafka = kafka_manager
        self.redis = redis_manager
        self.logger = logger
        self.buffer_size = buffer_size
        
        # Sliding window buffers for real-time analytics
        self.order_buffer = deque(maxlen=buffer_size)
        self.customer_buffer = deque(maxlen=buffer_size)
        self.product_buffer = deque(maxlen=buffer_size)
        
        # Real-time metrics
        self.metrics = defaultdict(int)
        self.processed_count = 0
        
        # Anomaly detection thresholds
        self.anomaly_thresholds = {
            'high_order_value': 1000.0,
            'bulk_orders_per_minute': 10,
            'rapid_stock_depletion': 50  # units per minute
        }
    
    def process_streaming_event(self, message: Dict[str, Any]) -> bool:
        """Process a single streaming event"""
        try:
            payload = message.get('payload', {})
            operation = payload.get('op')
            source = payload.get('source', {})
            table = source.get('table')
            after_data = payload.get('after')
            
            if operation != 'c' or not after_data:  # Only process new records (creates)
                return True
            
            event_data = {
                'table': table,
                'data': after_data,
                'timestamp': datetime.now(),
                'operation': operation
            }
            
            # Route to appropriate buffer and processor
            if table == 'orders':
                self.process_order_stream(event_data)
            elif table == 'customers':
                self.process_customer_stream(event_data)
            elif table == 'products':
                self.process_product_stream(event_data)
            elif table == 'order_items':
                self.process_order_item_stream(event_data)
            
            self.processed_count += 1
            
            # Trigger real-time analytics every 100 events
            if self.processed_count % 100 == 0:
                self.compute_real_time_metrics()
                self.detect_anomalies()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing streaming event: {e}")
            return False
    
    def process_order_stream(self, event_data: Dict[str, Any]):
        """Process order stream events"""
        order_data = event_data['data']
        timestamp = event_data['timestamp']
        
        # Add to buffer
        order_event = {
            'id': order_data.get('id'),
            'customer_id': order_data.get('customer_id'),
            'total_amount': float(order_data.get('total_amount', 0)),
            'status': order_data.get('status'),
            'timestamp': timestamp
        }
        self.order_buffer.append(order_event)
        
        # Update real-time metrics
        self.metrics['orders_per_minute'] += 1
        self.metrics['revenue_per_minute'] += order_event['total_amount']
        
        # Check for high-value order alert
        if order_event['total_amount'] > self.anomaly_thresholds['high_order_value']:
            self.send_alert('high_value_order', {
                'order_id': order_event['id'],
                'amount': order_event['total_amount'],
                'customer_id': order_event['customer_id'],
                'timestamp': timestamp.isoformat()
            })
        
        # Update customer activity in real-time
        if order_event['customer_id']:
            customer_activity_key = f"realtime:customer:{order_event['customer_id']}:activity"
            activity_data = {
                'last_order_time': timestamp.isoformat(),
                'last_order_amount': order_event['total_amount'],
                'order_count_today': 1
            }
            
            # Get existing activity and update
            existing_activity = self.redis.get_data(customer_activity_key) or {}
            if existing_activity:
                activity_data['order_count_today'] = existing_activity.get('order_count_today', 0) + 1
            
            self.redis.set_data(customer_activity_key, activity_data, expiry=86400)  # 24 hours
        
        self.logger.debug(f"Processed order stream event: {order_event['id']}")
    
    def process_customer_stream(self, event_data: Dict[str, Any]):
        """Process customer stream events"""
        customer_data = event_data['data']
        timestamp = event_data['timestamp']
        
        customer_event = {
            'id': customer_data.get('id'),
            'email': customer_data.get('email'),
            'city': customer_data.get('city'),
            'state': customer_data.get('state'),
            'timestamp': timestamp
        }
        self.customer_buffer.append(customer_event)
        
        # Update real-time metrics
        self.metrics['new_customers_per_minute'] += 1
        
        # Track geographic distribution in real-time
        if customer_event['state']:
            geo_key = f"realtime:geo:customers:{customer_event['state']}"
            self.redis.increment_counter(geo_key)
            # Set expiry for daily rollover
            redis_client = self.redis.get_client()
            redis_client.expire(geo_key, 86400)
        
        self.logger.debug(f"Processed customer stream event: {customer_event['id']}")
    
    def process_product_stream(self, event_data: Dict[str, Any]):
        """Process product stream events"""
        product_data = event_data['data']
        timestamp = event_data['timestamp']
        
        product_event = {
            'id': product_data.get('id'),
            'name': product_data.get('name'),
            'category': product_data.get('category'),
            'price': float(product_data.get('price', 0)),
            'stock_quantity': int(product_data.get('stock_quantity', 0)),
            'timestamp': timestamp
        }
        self.product_buffer.append(product_event)
        
        # Update real-time metrics
        self.metrics['new_products_per_minute'] += 1
        
        # Track category distribution
        if product_event['category']:
            category_key = f"realtime:categories:{product_event['category']}"
            self.redis.increment_counter(category_key)
            redis_client = self.redis.get_client()
            redis_client.expire(category_key, 86400)
        
        self.logger.debug(f"Processed product stream event: {product_event['id']}")
    
    def process_order_item_stream(self, event_data: Dict[str, Any]):
        """Process order item stream events"""
        item_data = event_data['data']
        timestamp = event_data['timestamp']
        
        item_event = {
            'order_id': item_data.get('order_id'),
            'product_id': item_data.get('product_id'),
            'quantity': int(item_data.get('quantity', 0)),
            'unit_price': float(item_data.get('unit_price', 0)),
            'timestamp': timestamp
        }
        
        # Update product velocity tracking
        if item_event['product_id'] and item_event['quantity']:
            velocity_key = f"realtime:product:{item_event['product_id']}:velocity"
            
            # Get current velocity data
            velocity_data = self.redis.get_data(velocity_key) or {'sales_per_minute': 0, 'window_start': timestamp.isoformat()}
            velocity_data['sales_per_minute'] += item_event['quantity']
            
            self.redis.set_data(velocity_key, velocity_data, expiry=300)  # 5 minutes
            
            # Check for rapid depletion
            if velocity_data['sales_per_minute'] > self.anomaly_thresholds['rapid_stock_depletion']:
                self.send_alert('rapid_stock_depletion', {
                    'product_id': item_event['product_id'],
                    'velocity': velocity_data['sales_per_minute'],
                    'timestamp': timestamp.isoformat()
                })
        
        self.logger.debug(f"Processed order item stream event for product: {item_event['product_id']}")
    
    def compute_real_time_metrics(self):
        """Compute and store real-time metrics"""
        current_time = datetime.now()
        
        # Calculate metrics for the last minute
        minute_ago = current_time - timedelta(minutes=1)
        
        # Orders in last minute
        recent_orders = [o for o in self.order_buffer if o['timestamp'] > minute_ago]
        orders_per_minute = len(recent_orders)
        revenue_per_minute = sum(o['total_amount'] for o in recent_orders)
        
        # Customers in last minute
        recent_customers = [c for c in self.customer_buffer if c['timestamp'] > minute_ago]
        customers_per_minute = len(recent_customers)
        
        # Real-time metrics
        rt_metrics = {
            'timestamp': current_time.isoformat(),
            'orders_per_minute': orders_per_minute,
            'revenue_per_minute': revenue_per_minute,
            'customers_per_minute': customers_per_minute,
            'avg_order_value': revenue_per_minute / orders_per_minute if orders_per_minute > 0 else 0,
            'buffer_sizes': {
                'orders': len(self.order_buffer),
                'customers': len(self.customer_buffer),
                'products': len(self.product_buffer)
            }
        }
        
        # Store in Redis with 1-hour expiry
        metrics_key = f"realtime:metrics:{current_time.strftime('%Y%m%d_%H%M')}"
        self.redis.set_data(metrics_key, rt_metrics, expiry=3600)
        
        # Also store as latest
        self.redis.set_data('realtime:metrics:latest', rt_metrics, expiry=300)
        
        self.logger.info(f"Real-time metrics: {rt_metrics}")
    
    def detect_anomalies(self):
        """Detect anomalies in streaming data"""
        current_time = datetime.now()
        minute_ago = current_time - timedelta(minutes=1)
        
        # Check for bulk orders (potential fraud or system testing)
        recent_orders = [o for o in self.order_buffer if o['timestamp'] > minute_ago]
        orders_per_minute = len(recent_orders)
        
        if orders_per_minute > self.anomaly_thresholds['bulk_orders_per_minute']:
            self.send_alert('bulk_orders_detected', {
                'orders_per_minute': orders_per_minute,
                'threshold': self.anomaly_thresholds['bulk_orders_per_minute'],
                'timestamp': current_time.isoformat()
            })
        
        # Check for unusual geographic patterns
        if len(self.customer_buffer) > 10:
            recent_customers = [c for c in self.customer_buffer if c['timestamp'] > minute_ago]
            if recent_customers:
                states = [c['state'] for c in recent_customers if c['state']]
                if states:
                    # Check if more than 80% of recent customers are from the same state
                    state_counts = {}
                    for state in states:
                        state_counts[state] = state_counts.get(state, 0) + 1
                    
                    max_state_count = max(state_counts.values())
                    if max_state_count / len(states) > 0.8 and len(states) > 5:
                        dominant_state = max(state_counts, key=state_counts.get)
                        self.send_alert('geographic_anomaly', {
                            'dominant_state': dominant_state,
                            'percentage': (max_state_count / len(states)) * 100,
                            'customer_count': len(states),
                            'timestamp': current_time.isoformat()
                        })
    
    def send_alert(self, alert_type: str, alert_data: Dict[str, Any]):
        """Send real-time alert"""
        alert = {
            'type': alert_type,
            'data': alert_data,
            'severity': self.get_alert_severity(alert_type),
            'timestamp': datetime.now().isoformat()
        }
        
        # Store alert in Redis
        alert_key = f"alerts:{alert_type}:{int(time.time())}"
        self.redis.set_data(alert_key, alert, expiry=86400)  # 24 hours
        
        # Send to Kafka alerts topic
        try:
            producer = self.kafka.get_producer()
            producer.send('realtime-alerts', value=alert, key=alert_type)
            self.logger.warning(f"ALERT [{alert['severity']}] {alert_type}: {alert_data}")
        except Exception as e:
            self.logger.error(f"Failed to send alert to Kafka: {e}")
    
    def get_alert_severity(self, alert_type: str) -> str:
        """Get alert severity level"""
        severity_map = {
            'high_value_order': 'MEDIUM',
            'bulk_orders_detected': 'HIGH',
            'rapid_stock_depletion': 'HIGH',
            'geographic_anomaly': 'LOW'
        }
        return severity_map.get(alert_type, 'MEDIUM')
    
    def get_stream_stats(self) -> Dict[str, Any]:
        """Get current streaming statistics"""
        current_time = datetime.now()
        
        stats = {
            'processed_count': self.processed_count,
            'buffer_sizes': {
                'orders': len(self.order_buffer),
                'customers': len(self.customer_buffer),
                'products': len(self.product_buffer)
            },
            'current_metrics': self.metrics.copy(),
            'timestamp': current_time.isoformat()
        }
        
        # Get latest real-time metrics
        latest_metrics = self.redis.get_data('realtime:metrics:latest')
        if latest_metrics:
            stats['latest_realtime_metrics'] = latest_metrics
        
        return stats

def main():
    """Main stream processor loop"""
    logger = setup_logging()
    db_config, kafka_config, redis_config, app_config = get_config()
    
    logger.info("Starting stream processor...")
    
    # Setup managers
    kafka_manager = KafkaManager(kafka_config)
    redis_manager = RedisManager(redis_config)
    
    processor = StreamProcessor(
        kafka_manager, 
        redis_manager, 
        logger, 
        buffer_size=app_config.stream_buffer_size
    )
    
    # Topics to consume from (same as CDC but different processing)
    topics = list(kafka_config.topics.values())
    
    logger.info(f"Subscribing to topics for streaming: {topics}")
    
    try:
        # Create consumer with different group ID
        consumer = kafka_manager.get_consumer(topics, group_id='stream-processor-group')
        
        logger.info("Stream processor ready, waiting for messages...")
        
        message_count = 0
        for message in consumer:
            try:
                if message.value:
                    processor.process_streaming_event(message.value)
                    message_count += 1
                    
                    # Log stats every 500 messages
                    if message_count % 500 == 0:
                        stats = processor.get_stream_stats()
                        logger.info(f"Processed {message_count} streaming messages. Stats: {stats}")
                
            except Exception as e:
                logger.error(f"Error processing streaming message: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Stream processor stopped by user")
    except Exception as e:
        logger.error(f"Stream processor error: {e}")
    finally:
        kafka_manager.close()
        redis_manager.close()
        logger.info("Stream processor shut down")

if __name__ == "__main__":
    main()