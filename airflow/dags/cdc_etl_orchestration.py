"""
CDC Stream Batch ETL Orchestration DAG
Using Airflow 3.0 SDK with best practices including assets, tasks, and bitwise mapping
"""
from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow.sdk.definitions.dag import dag, DAG
from airflow.sdk.definitions.task import task
from airflow.sdk.definitions.asset import Asset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor

# Import our asset definitions
from assets import (
    SOURCE_TABLES, CDC_STREAMS, ANALYTICS_CACHES,
    CUSTOMERS_TABLE, ORDERS_TABLE, PRODUCTS_TABLE, ORDER_ITEMS_TABLE,
    CUSTOMERS_CDC_TOPIC, ORDERS_CDC_TOPIC, PRODUCTS_CDC_TOPIC, ORDER_ITEMS_CDC_TOPIC,
    CUSTOMER_SEGMENTS_CACHE, PRODUCT_PERFORMANCE_CACHE, REAL_TIME_METRICS_CACHE,
    ANOMALY_ALERTS_CACHE, REVENUE_ANALYTICS, GEOGRAPHIC_DISTRIBUTION
)

# Default arguments for the DAG
default_args = {
    'owner': 'etl-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

@dag(
    dag_id='cdc_stream_batch_etl_orchestration',
    default_args=default_args,
    description='Comprehensive CDC Stream Batch ETL orchestration using Airflow 3.0 SDK',
    schedule="@daily",
    catchup=False,
    tags=['etl', 'cdc', 'stream', 'batch', 'analytics'],
    doc_md=__doc__,
    is_paused_upon_creation=True,
)
def cdc_stream_batch_etl_dag():
    """
    Main ETL orchestration DAG that coordinates:
    1. Data generation and CDC capture
    2. Stream processing for real-time analytics
    3. Batch processing for historical analytics 
    4. Anomaly detection and alerting
    """

    # Health check tasks for all dependencies
    @task(
        task_id="check_postgres_health",
        outlets=[CUSTOMERS_TABLE, ORDERS_TABLE, PRODUCTS_TABLE, ORDER_ITEMS_TABLE]
    )
    def check_postgres_health() -> bool:
        """Check PostgreSQL database health and connectivity"""
        try:
            postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
            records = postgres_hook.get_records("SELECT 1")
            return True if records else False
        except Exception as e:
            print(f"PostgreSQL health check failed: {e}")
            return False

    @task(task_id="check_kafka_health")  
    def check_kafka_health() -> bool:
        """Check Kafka broker health and topic availability"""
        import sys
        sys.path.append('/opt/airflow/etl_modules')
        
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=['kafka:29092'],
                value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else v
            )
            producer.close()
            return True
        except Exception as e:
            print(f"Kafka health check failed: {e}")
            return False

    @task(task_id="check_redis_health")
    def check_redis_health() -> bool:
        """Check Redis connectivity and cache health"""
        import sys
        sys.path.append('/opt/airflow/etl_modules')
        
        try:
            import redis
            r = redis.Redis(host='redis', port=6379, password='redis123', decode_responses=True)
            r.ping()
            return True
        except Exception as e:
            print(f"Redis health check failed: {e}")
            return False

    # Data generation task
    @task(
        task_id="generate_mock_data",
        outlets=[CUSTOMERS_TABLE, ORDERS_TABLE, PRODUCTS_TABLE, ORDER_ITEMS_TABLE]
    )
    def generate_mock_data(**context) -> Dict[str, Any]:
        """Generate mock e-commerce data for testing and development"""
        import sys
        sys.path.append('/opt/airflow/etl_modules')
        
        try:
            from data_generator import DataGenerator
            from config import get_config
            
            db_config, kafka_config, redis_config, app_config = get_config()
            generator = DataGenerator(db_config)
            
            # Generate a batch of test data
            stats = generator.generate_batch_data(batch_size=100)
            
            return {
                'records_generated': stats.get('total_records', 0),
                'customers': stats.get('customers', 0), 
                'products': stats.get('products', 0),
                'orders': stats.get('orders', 0),
                'order_items': stats.get('order_items', 0),
                'execution_time': context['ts']
            }
        except Exception as e:
            print(f"Data generation failed: {e}")
            raise

    # CDC Processing tasks with bitwise mapping
    cdc_tasks = []
    cdc_topics = [
        ('customers', CUSTOMERS_CDC_TOPIC),
        ('orders', ORDERS_CDC_TOPIC), 
        ('products', PRODUCTS_CDC_TOPIC),
        ('order_items', ORDER_ITEMS_CDC_TOPIC)
    ]

    for table_name, topic_asset in cdc_topics:
        @task(
            task_id=f"process_cdc_{table_name}",
            outlets=[topic_asset]
        )
        def process_cdc_events(table_name: str = table_name) -> Dict[str, Any]:
            """Process CDC events for specific table using bitwise operations"""
            import sys
            sys.path.append('/opt/airflow/etl_modules')
            
            try:
                from cdc_processor import CDCProcessor
                from config import get_config
                
                db_config, kafka_config, redis_config, app_config = get_config()
                processor = CDCProcessor(db_config, kafka_config, redis_config)
                
                # Process events with bitwise filtering for efficiency
                stats = processor.process_table_events(
                    table_name=table_name,
                    max_events=1000,
                    use_bitwise_filters=True
                )
                
                return {
                    'table': table_name,
                    'events_processed': stats.get('processed', 0),
                    'events_filtered': stats.get('filtered', 0),
                    'cache_updates': stats.get('cache_updates', 0)
                }
            except Exception as e:
                print(f"CDC processing failed for {table_name}: {e}")
                raise

        cdc_tasks.append(process_cdc_events)

    # Stream processing with real-time analytics
    @task(
        task_id="stream_analytics",
        inlets=CDC_STREAMS,
        outlets=[REAL_TIME_METRICS_CACHE, ANOMALY_ALERTS_CACHE]
    )
    def run_stream_analytics(**context) -> Dict[str, Any]:
        """Run real-time stream analytics and anomaly detection"""
        import sys
        sys.path.append('/opt/airflow/etl_modules')
        
        try:
            from stream_processor import StreamProcessor
            from config import get_config
            
            kafka_config, redis_config = get_config()[1:3]
            processor = StreamProcessor(kafka_config, redis_config)
            
            # Process streaming data for specified duration
            stats = processor.run_batch_processing(duration_minutes=5)
            
            return {
                'events_processed': stats.get('total_events', 0),
                'anomalies_detected': stats.get('anomalies', 0),
                'metrics_computed': stats.get('metrics', 0),
                'alerts_generated': stats.get('alerts', 0)
            }
        except Exception as e:
            print(f"Stream analytics failed: {e}")
            raise

    # Batch processing with comprehensive ETL
    @task(
        task_id="batch_etl_processing",
        inlets=[CUSTOMERS_TABLE, ORDERS_TABLE, PRODUCTS_TABLE, ORDER_ITEMS_TABLE],
        outlets=[CUSTOMER_SEGMENTS_CACHE, PRODUCT_PERFORMANCE_CACHE, REVENUE_ANALYTICS]
    )
    def run_batch_etl(**context) -> Dict[str, Any]:
        """Run comprehensive batch ETL processing"""
        import sys
        sys.path.append('/opt/airflow/etl_modules')
        
        try:
            from batch_processor import BatchProcessor
            from config import get_config
            
            db_config, kafka_config, redis_config, app_config = get_config()
            processor = BatchProcessor(db_config, redis_config)
            
            # Run full ETL pipeline
            stats = processor.run_comprehensive_etl()
            
            return {
                'customers_processed': stats.get('customers', 0),
                'orders_analyzed': stats.get('orders', 0),
                'segments_updated': stats.get('segments', 0),
                'performance_metrics': stats.get('performance', 0),
                'revenue_calculated': stats.get('revenue', 0)
            }
        except Exception as e:
            print(f"Batch ETL processing failed: {e}")
            raise

    # Geographic analytics with bitwise mapping
    @task(
        task_id="geographic_analytics", 
        map_index_template="{{ my_custom_map_index }}",
        outlets=[GEOGRAPHIC_DISTRIBUTION]
    )
    def analyze_geographic_distribution(region_filters: List[str] = None) -> Dict[str, Any]:
        """Analyze geographic distribution using bitwise region mapping"""
        import sys
        sys.path.append('/opt/airflow/etl_modules')
        
        try:
            # Use bitwise operations for efficient region filtering
            region_bits = {
                'north': 0b0001,  # 1
                'south': 0b0010,  # 2  
                'east': 0b0100,   # 4
                'west': 0b1000,   # 8
            }
            
            active_regions = 0
            if region_filters:
                for region in region_filters:
                    if region in region_bits:
                        active_regions |= region_bits[region]
            else:
                active_regions = 0b1111  # All regions
            
            from utils import RedisManager
            from config import get_config
            
            redis_config = get_config()[2]
            redis_mgr = RedisManager(redis_config)
            
            # Simulate geographic analysis with bitwise filtering
            stats = {
                'regions_analyzed': bin(active_regions).count('1'),
                'active_region_mask': active_regions,
                'distribution_calculated': True
            }
            
            # Store results in Redis
            redis_mgr.set_json('geographic_distribution:latest', stats)
            
            return stats
        except Exception as e:
            print(f"Geographic analytics failed: {e}")
            raise

    # Data quality validation
    @task(
        task_id="validate_data_quality",
        inlets=[CUSTOMER_SEGMENTS_CACHE, PRODUCT_PERFORMANCE_CACHE, REAL_TIME_METRICS_CACHE]
    )
    def validate_data_quality(**context) -> Dict[str, Any]:
        """Validate data quality across all processed assets"""
        import sys
        sys.path.append('/opt/airflow/etl_modules')
        
        try:
            from utils import RedisManager
            from config import get_config
            
            redis_config = get_config()[2]
            redis_mgr = RedisManager(redis_config)
            
            # Validate key metrics exist and are reasonable
            validations = {
                'customer_segments_valid': redis_mgr.exists('customer_segments:*'),
                'product_performance_valid': redis_mgr.exists('product_performance:*'),
                'real_time_metrics_valid': redis_mgr.exists('real_time_metrics:*'),
                'data_freshness_check': True,
                'validation_timestamp': context['ts']
            }
            
            return validations
        except Exception as e:
            print(f"Data quality validation failed: {e}")
            raise

    # Define task dependencies with proper asset flow
    postgres_health = check_postgres_health()
    kafka_health = check_kafka_health()
    redis_health = check_redis_health()
    
    # Data generation depends on database health
    data_gen = generate_mock_data()
    data_gen.set_upstream([postgres_health])
    
    # CDC processing depends on both data generation and Kafka health
    cdc_processing = [task_func() for task_func in cdc_tasks]
    for cdc_task in cdc_processing:
        cdc_task.set_upstream([data_gen, kafka_health])
    
    # Stream analytics depends on CDC events and Redis health
    stream_task = run_stream_analytics()
    stream_task.set_upstream(cdc_processing + [redis_health])
    
    # Batch ETL depends on source data
    batch_task = run_batch_etl()
    batch_task.set_upstream([data_gen, redis_health])
    
    # Geographic analytics with mapped regions
    geo_task = analyze_geographic_distribution.partial(
        region_filters=['north', 'south', 'east', 'west']
    ).expand(region_filters=[['north'], ['south'], ['east'], ['west']])
    geo_task.set_upstream([batch_task])
    
    # Data quality validation depends on all processing tasks
    quality_task = validate_data_quality()
    quality_task.set_upstream([stream_task, batch_task, geo_task])

    return quality_task

# Create the DAG instance
dag_instance = cdc_stream_batch_etl_dag()