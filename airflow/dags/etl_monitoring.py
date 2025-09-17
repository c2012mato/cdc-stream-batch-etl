"""
System Monitoring and Health Check DAG
Using Airflow 3.0 SDK for continuous monitoring of ETL system components
"""
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow.sdk.definitions.dag import dag
from airflow.sdk.definitions.task import task
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

from assets import ALL_ASSETS, ANALYTICS_CACHES

default_args = {
    'owner': 'sre-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='etl_system_monitoring',
    default_args=default_args,
    description='Continuous monitoring and health checks for ETL system',
    schedule=timedelta(minutes=15),  # Run every 15 minutes
    catchup=False,
    tags=['monitoring', 'health', 'sre'],
    max_active_runs=1,
)
def etl_monitoring_dag():
    """
    Monitoring DAG that continuously checks:
    1. Service health (Postgres, Kafka, Redis, Debezium)
    2. Data flow metrics
    3. Performance indicators
    4. Alert on anomalies
    """

    @task(task_id="check_debezium_connector")
    def check_debezium_health() -> Dict[str, Any]:
        """Check Debezium connector status via REST API"""
        import requests
        import json
        
        try:
            # Check connector status
            response = requests.get(
                'http://debezium:8083/connectors/postgres-source-connector/status',
                timeout=10
            )
            
            if response.status_code == 200:
                status = response.json()
                return {
                    'connector_status': status.get('connector', {}).get('state'),
                    'tasks_running': len([t for t in status.get('tasks', []) if t.get('state') == 'RUNNING']),
                    'health': 'healthy' if status.get('connector', {}).get('state') == 'RUNNING' else 'unhealthy'
                }
            else:
                return {'health': 'unhealthy', 'error': f'HTTP {response.status_code}'}
                
        except Exception as e:
            return {'health': 'unhealthy', 'error': str(e)}

    @task(task_id="monitor_data_flow")
    def monitor_kafka_topics() -> Dict[str, Any]:
        """Monitor Kafka topic metrics and message flow"""
        import sys
        sys.path.append('/opt/airflow/etl_modules')
        
        try:
            from kafka import KafkaConsumer
            from kafka.errors import KafkaError
            
            topics = [
                'dbserver1.public.customers',
                'dbserver1.public.orders', 
                'dbserver1.public.products',
                'dbserver1.public.order_items'
            ]
            
            topic_stats = {}
            for topic in topics:
                try:
                    consumer = KafkaConsumer(
                        topic,
                        bootstrap_servers=['kafka:29092'],
                        auto_offset_reset='latest',
                        consumer_timeout_ms=5000
                    )
                    
                    # Get partition info
                    partitions = consumer.partitions_for_topic(topic)
                    topic_stats[topic] = {
                        'partitions': len(partitions) if partitions else 0,
                        'accessible': True
                    }
                    consumer.close()
                    
                except KafkaError as e:
                    topic_stats[topic] = {
                        'accessible': False,
                        'error': str(e)
                    }
            
            return {
                'topics_monitored': len(topics),
                'healthy_topics': len([t for t in topic_stats.values() if t.get('accessible')]),
                'topic_details': topic_stats
            }
            
        except Exception as e:
            return {'error': str(e), 'healthy_topics': 0}

    @task(task_id="check_cache_performance")
    def monitor_redis_performance() -> Dict[str, Any]:
        """Monitor Redis cache performance and memory usage"""
        import sys
        sys.path.append('/opt/airflow/etl_modules')
        
        try:
            import redis
            
            r = redis.Redis(host='redis', port=6379, password='redis123', decode_responses=True)
            
            # Get Redis info
            info = r.info()
            
            # Check key patterns for our caches
            cache_stats = {}
            patterns = [
                'customer_segments:*',
                'product_performance:*', 
                'real_time_metrics:*',
                'anomaly_alerts:*'
            ]
            
            for pattern in patterns:
                keys = r.keys(pattern)
                cache_stats[pattern] = len(keys)
            
            return {
                'memory_used_mb': round(info.get('used_memory', 0) / 1024 / 1024, 2),
                'connected_clients': info.get('connected_clients', 0),
                'cache_hit_rate': info.get('keyspace_hits', 0) / max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1),
                'cache_patterns': cache_stats,
                'total_keys': sum(cache_stats.values())
            }
            
        except Exception as e:
            return {'error': str(e), 'healthy': False}

    @task(task_id="check_processor_health")
    def monitor_processor_containers() -> Dict[str, Any]:
        """Monitor ETL processor container health via docker stats"""
        import subprocess
        import json
        
        try:
            processors = [
                'cdc-processor',
                'batch-processor', 
                'stream-processor',
                'data-generator'
            ]
            
            container_stats = {}
            for processor in processors:
                try:
                    # Use docker inspect to get container status
                    result = subprocess.run(
                        ['docker', 'inspect', processor, '--format={{.State.Status}}'],
                        capture_output=True, text=True, timeout=10
                    )
                    
                    if result.returncode == 0:
                        status = result.stdout.strip()
                        container_stats[processor] = {
                            'status': status,
                            'healthy': status == 'running'
                        }
                    else:
                        container_stats[processor] = {
                            'status': 'not_found',
                            'healthy': False
                        }
                    
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    container_stats[processor] = {
                        'status': 'unknown',
                        'healthy': False
                    }
            
            healthy_count = len([c for c in container_stats.values() if c.get('healthy')])
            
            return {
                'total_processors': len(processors),
                'healthy_processors': healthy_count,
                'container_details': container_stats,
                'overall_health': 'healthy' if healthy_count == len(processors) else 'degraded'
            }
            
        except Exception as e:
            return {'error': str(e), 'overall_health': 'unknown'}

    @task(task_id="performance_metrics")
    def collect_performance_metrics() -> Dict[str, Any]:
        """Collect overall system performance metrics"""
        import sys
        sys.path.append('/opt/airflow/etl_modules')
        
        try:
            from utils import RedisManager
            from config import get_config
            
            redis_config = get_config()[2]
            redis_mgr = RedisManager(redis_config)
            
            # Collect various performance counters
            metrics = {
                'batch_jobs_completed': redis_mgr.get_counter('batch_jobs:completed') or 0,
                'batch_jobs_failed': redis_mgr.get_counter('batch_jobs:failed') or 0,
                'cdc_events_processed': redis_mgr.get_counter('cdc_events:processed') or 0,
                'stream_events_processed': redis_mgr.get_counter('stream_events:processed') or 0,
                'anomalies_detected': redis_mgr.get_counter('anomalies:detected') or 0,
                'data_quality_score': redis_mgr.get_counter('data_quality:score') or 0
            }
            
            # Calculate success rates
            total_batch_jobs = metrics['batch_jobs_completed'] + metrics['batch_jobs_failed']
            metrics['batch_success_rate'] = (metrics['batch_jobs_completed'] / max(total_batch_jobs, 1)) * 100
            
            return metrics
            
        except Exception as e:
            return {'error': str(e)}

    @task(task_id="generate_health_report")
    def generate_system_health_report(
        debezium_health: Dict[str, Any],
        kafka_metrics: Dict[str, Any], 
        redis_performance: Dict[str, Any],
        container_health: Dict[str, Any],
        performance_metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate comprehensive system health report"""
        
        # Calculate overall system health score
        health_score = 0
        max_score = 100
        
        # Debezium health (20 points)
        if debezium_health.get('health') == 'healthy':
            health_score += 20
            
        # Kafka topics health (20 points)
        kafka_ratio = kafka_metrics.get('healthy_topics', 0) / max(kafka_metrics.get('topics_monitored', 1), 1)
        health_score += kafka_ratio * 20
        
        # Redis performance (20 points) 
        if not redis_performance.get('error'):
            health_score += 20
            
        # Container health (20 points)
        if container_health.get('overall_health') == 'healthy':
            health_score += 20
        elif container_health.get('overall_health') == 'degraded':
            health_score += 10
            
        # Performance metrics (20 points)
        if not performance_metrics.get('error'):
            success_rate = performance_metrics.get('batch_success_rate', 0)
            health_score += (success_rate / 100) * 20
        
        # Generate health status
        if health_score >= 90:
            status = 'excellent'
        elif health_score >= 70:
            status = 'good'
        elif health_score >= 50:
            status = 'warning'
        else:
            status = 'critical'
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'overall_health_score': round(health_score, 2),
            'health_status': status,
            'component_health': {
                'debezium': debezium_health,
                'kafka': kafka_metrics,
                'redis': redis_performance, 
                'containers': container_health,
                'performance': performance_metrics
            },
            'recommendations': []
        }
        
        # Generate recommendations based on health issues
        if debezium_health.get('health') != 'healthy':
            report['recommendations'].append('Check Debezium connector configuration and restart if needed')
            
        if kafka_metrics.get('healthy_topics', 0) < kafka_metrics.get('topics_monitored', 0):
            report['recommendations'].append('Investigate Kafka topic connectivity issues')
            
        if redis_performance.get('error'):
            report['recommendations'].append('Check Redis connectivity and memory usage')
            
        if container_health.get('overall_health') != 'healthy':
            report['recommendations'].append('Restart unhealthy processor containers')
            
        return report

    # Define task flow
    debezium_check = check_debezium_health()
    kafka_monitor = monitor_kafka_topics()
    redis_monitor = monitor_redis_performance()
    container_monitor = monitor_processor_containers()
    perf_metrics = collect_performance_metrics()
    
    health_report = generate_system_health_report(
        debezium_check,
        kafka_monitor,
        redis_monitor, 
        container_monitor,
        perf_metrics
    )

    return health_report

# Create the monitoring DAG instance
monitoring_dag_instance = etl_monitoring_dag()