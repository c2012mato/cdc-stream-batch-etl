"""
Airflow utilities and extensions for existing ETL modules
Provides enhanced functionality for Airflow integration
"""
import os
import sys
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

# Add ETL modules to path
sys.path.append('/opt/airflow/etl_modules')

from config import get_config
from utils import DatabaseConnection, RedisManager, KafkaManager

class AirflowETLBase:
    """Base class for Airflow-integrated ETL operations"""
    
    def __init__(self):
        self.db_config, self.kafka_config, self.redis_config, self.app_config = get_config()
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for Airflow integration"""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(getattr(logging, self.app_config.log_level))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger

class AirflowCDCProcessor(AirflowETLBase):
    """CDC Processor with Airflow-specific enhancements"""
    
    def __init__(self):
        super().__init__()
        self.db_connection = None
        self.redis_manager = None
        self.kafka_manager = None
        
    def __enter__(self):
        """Context manager entry"""
        self.db_connection = DatabaseConnection(self.db_config)
        self.db_connection.connect()
        self.redis_manager = RedisManager(self.redis_config)
        self.kafka_manager = KafkaManager(self.kafka_config)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.db_connection:
            self.db_connection.close()
        if self.redis_manager:
            self.redis_manager.close()
        if self.kafka_manager:
            self.kafka_manager.close()
    
    def process_table_events(self, table_name: str, max_events: int = 1000, 
                           use_bitwise_filters: bool = True) -> Dict[str, Any]:
        """Process CDC events for a specific table with enhanced filtering"""
        
        topic_name = f"dbserver1.public.{table_name}"
        processed_count = 0
        filtered_count = 0
        cache_updates = 0
        
        try:
            # Create consumer for specific topic
            consumer = self.kafka_manager.create_consumer([topic_name])
            
            # Process messages with timeout
            start_time = datetime.now()
            timeout = timedelta(minutes=5)
            
            for message in consumer:
                if datetime.now() - start_time > timeout:
                    break
                    
                if processed_count >= max_events:
                    break
                
                # Apply bitwise filtering if enabled
                if use_bitwise_filters and self._should_filter_event(message.value, table_name):
                    filtered_count += 1
                    continue
                
                # Process the event
                if self._process_single_event(message.value, table_name):
                    processed_count += 1
                    cache_updates += self._update_cache(message.value, table_name)
                    
            consumer.close()
            
            # Update metrics
            self.redis_manager.increment_counter('cdc_events:processed', processed_count)
            
            return {
                'processed': processed_count,
                'filtered': filtered_count,
                'cache_updates': cache_updates,
                'topic': topic_name
            }
            
        except Exception as e:
            self.logger.error(f"CDC processing failed for {table_name}: {e}")
            raise
    
    def _should_filter_event(self, event_data: Dict[str, Any], table_name: str) -> bool:
        """Apply bitwise filtering to determine if event should be processed"""
        # Example bitwise filtering logic
        filter_mask = 0b1111  # Default: process all events
        
        if table_name == 'customers':
            # Only process customer creation and updates (not deletes)
            operation = event_data.get('op', '')
            if operation in ['c', 'u']:  # create, update
                return False  # Don't filter
            else:
                return True   # Filter out
                
        elif table_name == 'orders':
            # Only process orders above certain value
            order_value = event_data.get('after', {}).get('total_amount', 0)
            return order_value < 10.0  # Filter small orders
            
        return False  # Don't filter by default
    
    def _process_single_event(self, event_data: Dict[str, Any], table_name: str) -> bool:
        """Process a single CDC event"""
        try:
            # Log the event processing
            self.logger.debug(f"Processing {table_name} event: {event_data.get('op', 'unknown')}")
            
            # Simulate processing logic
            operation = event_data.get('op', '')
            if operation in ['c', 'u', 'd']:
                return True
                
            return False
        except Exception as e:
            self.logger.error(f"Failed to process event: {e}")
            return False
    
    def _update_cache(self, event_data: Dict[str, Any], table_name: str) -> int:
        """Update cache with processed event data"""
        try:
            cache_key = f"{table_name}:latest_events"
            
            # Store latest event timestamp
            self.redis_manager.set(
                f"{cache_key}:timestamp",
                datetime.now().isoformat()
            )
            
            return 1
        except Exception as e:
            self.logger.error(f"Cache update failed: {e}")
            return 0

class AirflowBatchProcessor(AirflowETLBase):
    """Batch Processor with Airflow-specific enhancements"""
    
    def __init__(self):
        super().__init__()
        self.db_connection = None
        self.redis_manager = None
        
    def __enter__(self):
        """Context manager entry"""
        self.db_connection = DatabaseConnection(self.db_config)
        self.db_connection.connect()
        self.redis_manager = RedisManager(self.redis_config)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.db_connection:
            self.db_connection.close()
        if self.redis_manager:
            self.redis_manager.close()
    
    def run_comprehensive_etl(self) -> Dict[str, Any]:
        """Run comprehensive ETL processing"""
        
        try:
            self.logger.info("Starting comprehensive ETL processing")
            
            # Extract data
            customers_data = self._extract_customer_data()
            orders_data = self._extract_orders_data()
            products_data = self._extract_products_data()
            
            # Transform data
            customer_segments = self._transform_customer_segments(customers_data)
            product_performance = self._transform_product_performance(products_data, orders_data)
            revenue_analytics = self._calculate_revenue_analytics(orders_data)
            
            # Load results to cache
            self._load_analytics_to_cache(customer_segments, product_performance, revenue_analytics)
            
            # Update counters
            self.redis_manager.increment_counter('batch_jobs:completed')
            
            return {
                'customers': len(customers_data),
                'orders': len(orders_data),
                'segments': len(customer_segments),
                'performance': len(product_performance),
                'revenue': revenue_analytics.get('total_revenue', 0)
            }
            
        except Exception as e:
            self.redis_manager.increment_counter('batch_jobs:failed')
            self.logger.error(f"Comprehensive ETL failed: {e}")
            raise
    
    def _extract_customer_data(self) -> List[Dict[str, Any]]:
        """Extract customer data"""
        query = """
        SELECT customer_id, email, segment, state, total_spent
        FROM customers 
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        """
        return self.db_connection.execute_query(query)
    
    def _extract_orders_data(self) -> List[Dict[str, Any]]:
        """Extract orders data"""
        query = """
        SELECT order_id, customer_id, total_amount, order_date, status
        FROM orders 
        WHERE order_date >= NOW() - INTERVAL '24 hours'
        """
        return self.db_connection.execute_query(query)
    
    def _extract_products_data(self) -> List[Dict[str, Any]]:
        """Extract products data"""
        query = """
        SELECT product_id, name, category, price
        FROM products
        """
        return self.db_connection.execute_query(query)
    
    def _transform_customer_segments(self, customers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Transform customer data into segments"""
        segments = {'Gold': 0, 'Silver': 0, 'Bronze': 0}
        
        for customer in customers:
            segment = customer.get('segment', 'Bronze')
            segments[segment] = segments.get(segment, 0) + 1
            
        return segments
    
    def _transform_product_performance(self, products: List[Dict[str, Any]], 
                                     orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Transform product performance metrics"""
        performance = {}
        
        # Calculate metrics per product
        for product in products:
            product_id = product['product_id']
            product_orders = [o for o in orders if o.get('product_id') == product_id]
            
            performance[product_id] = {
                'name': product['name'],
                'category': product['category'],
                'orders_count': len(product_orders),
                'total_revenue': sum(o.get('total_amount', 0) for o in product_orders)
            }
            
        return performance
    
    def _calculate_revenue_analytics(self, orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate revenue analytics"""
        total_revenue = sum(o.get('total_amount', 0) for o in orders)
        order_count = len(orders)
        avg_order_value = total_revenue / max(order_count, 1)
        
        return {
            'total_revenue': total_revenue,
            'order_count': order_count,
            'avg_order_value': avg_order_value,
            'calculation_date': datetime.now().isoformat()
        }
    
    def _load_analytics_to_cache(self, customer_segments: Dict[str, Any],
                                product_performance: Dict[str, Any],
                                revenue_analytics: Dict[str, Any]):
        """Load analytics results to Redis cache"""
        
        # Store customer segments
        self.redis_manager.set_json('customer_segments:latest', customer_segments)
        
        # Store product performance
        self.redis_manager.set_json('product_performance:latest', product_performance)
        
        # Store revenue analytics
        self.redis_manager.set_json('revenue_analytics:latest', revenue_analytics)

class AirflowStreamProcessor(AirflowETLBase):
    """Stream Processor with Airflow-specific enhancements"""
    
    def __init__(self):
        super().__init__()
        self.kafka_manager = None
        self.redis_manager = None
        
    def __enter__(self):
        """Context manager entry"""
        self.kafka_manager = KafkaManager(self.kafka_config)
        self.redis_manager = RedisManager(self.redis_config)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.kafka_manager:
            self.kafka_manager.close()
        if self.redis_manager:
            self.redis_manager.close()
    
    def run_batch_processing(self, duration_minutes: int = 5) -> Dict[str, Any]:
        """Run stream processing for specified duration"""
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        events_processed = 0
        anomalies_detected = 0
        metrics_computed = 0
        alerts_generated = 0
        
        try:
            # Create consumer for all CDC topics
            topics = list(self.kafka_config.topics.values())
            consumer = self.kafka_manager.create_consumer(topics)
            
            while datetime.now() < end_time:
                for message in consumer:
                    if datetime.now() >= end_time:
                        break
                        
                    # Process streaming event
                    if self._process_streaming_event(message.value):
                        events_processed += 1
                        
                        # Check for anomalies every 100 events
                        if events_processed % 100 == 0:
                            anomalies = self._detect_anomalies()
                            anomalies_detected += len(anomalies)
                            alerts_generated += self._generate_alerts(anomalies)
                            
                            metrics_computed += self._compute_metrics()
            
            consumer.close()
            
            # Update counters
            self.redis_manager.increment_counter('stream_events:processed', events_processed)
            self.redis_manager.increment_counter('anomalies:detected', anomalies_detected)
            
            return {
                'total_events': events_processed,
                'anomalies': anomalies_detected,
                'metrics': metrics_computed,
                'alerts': alerts_generated
            }
            
        except Exception as e:
            self.logger.error(f"Stream processing failed: {e}")
            raise
    
    def _process_streaming_event(self, event_data: Dict[str, Any]) -> bool:
        """Process a single streaming event"""
        try:
            # Simulate stream processing
            self.logger.debug(f"Processing streaming event: {event_data.get('op', 'unknown')}")
            return True
        except Exception as e:
            self.logger.error(f"Stream event processing failed: {e}")
            return False
    
    def _detect_anomalies(self) -> List[Dict[str, Any]]:
        """Detect anomalies in streaming data"""
        # Simulate anomaly detection
        import random
        
        anomalies = []
        if random.random() < 0.1:  # 10% chance of anomaly
            anomalies.append({
                'type': 'high_value_order',
                'timestamp': datetime.now().isoformat(),
                'severity': 'medium'
            })
            
        return anomalies
    
    def _generate_alerts(self, anomalies: List[Dict[str, Any]]) -> int:
        """Generate alerts for detected anomalies"""
        alerts_count = 0
        
        for anomaly in anomalies:
            alert_key = f"anomaly_alerts:{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.redis_manager.set_json(alert_key, anomaly)
            alerts_count += 1
            
        return alerts_count
    
    def _compute_metrics(self) -> int:
        """Compute real-time metrics"""
        # Store current metrics
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'events_per_second': 10,  # Simulate
            'avg_processing_time': 0.05  # Simulate
        }
        
        self.redis_manager.set_json('real_time_metrics:latest', metrics)
        return 1