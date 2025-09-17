"""
Batch ETL Processor
Performs batch processing operations on data periodically
"""
import time
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
from utils import DatabaseConnection, RedisManager, setup_logging
from config import get_config

class BatchProcessor:
    """Perform batch ETL operations"""
    
    def __init__(self, db_connection: DatabaseConnection, redis_manager: RedisManager, logger: logging.Logger):
        self.db = db_connection
        self.redis = redis_manager
        self.logger = logger
        self.batch_count = 0
    
    def extract_customer_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Extract customer data for the specified date range"""
        query = """
        SELECT 
            c.id,
            c.first_name,
            c.last_name,
            c.email,
            c.city,
            c.state,
            c.created_at,
            COUNT(o.id) as total_orders,
            COALESCE(SUM(o.total_amount), 0) as total_spent,
            MAX(o.order_date) as last_order_date
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        WHERE c.created_at >= %s AND c.created_at <= %s
        GROUP BY c.id, c.first_name, c.last_name, c.email, c.city, c.state, c.created_at
        ORDER BY c.created_at DESC
        """
        
        try:
            results = self.db.execute_query(query, (start_date, end_date))
            df = pd.DataFrame(results)
            self.logger.info(f"Extracted {len(df)} customer records")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting customer data: {e}")
            return pd.DataFrame()
    
    def extract_product_performance(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Extract product performance data"""
        query = """
        SELECT 
            p.id,
            p.name,
            p.category,
            p.price,
            p.stock_quantity,
            COALESCE(SUM(oi.quantity), 0) as total_sold,
            COALESCE(SUM(oi.quantity * oi.unit_price), 0) as total_revenue,
            COUNT(DISTINCT o.id) as unique_orders
        FROM products p
        LEFT JOIN order_items oi ON p.id = oi.product_id
        LEFT JOIN orders o ON oi.order_id = o.id
        WHERE p.created_at >= %s AND p.created_at <= %s
           OR (o.order_date >= %s AND o.order_date <= %s)
        GROUP BY p.id, p.name, p.category, p.price, p.stock_quantity
        ORDER BY total_revenue DESC
        """
        
        try:
            results = self.db.execute_query(query, (start_date, end_date, start_date, end_date))
            df = pd.DataFrame(results)
            self.logger.info(f"Extracted {len(df)} product performance records")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting product performance data: {e}")
            return pd.DataFrame()
    
    def extract_order_analytics(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Extract order analytics data"""
        query = """
        SELECT 
            DATE(o.order_date) as order_date,
            o.status,
            COUNT(*) as order_count,
            SUM(o.total_amount) as total_revenue,
            AVG(o.total_amount) as avg_order_value,
            MIN(o.total_amount) as min_order_value,
            MAX(o.total_amount) as max_order_value
        FROM orders o
        WHERE o.order_date >= %s AND o.order_date <= %s
        GROUP BY DATE(o.order_date), o.status
        ORDER BY order_date DESC, o.status
        """
        
        try:
            results = self.db.execute_query(query, (start_date, end_date))
            df = pd.DataFrame(results)
            self.logger.info(f"Extracted {len(df)} order analytics records")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting order analytics data: {e}")
            return pd.DataFrame()
    
    def transform_customer_segments(self, customer_df: pd.DataFrame) -> pd.DataFrame:
        """Transform customer data to create customer segments"""
        if customer_df.empty:
            return customer_df
        
        # Create customer segments based on spending and order frequency
        customer_df['customer_segment'] = 'Bronze'
        
        # Calculate percentiles for segmentation
        if len(customer_df) > 0:
            spending_80th = customer_df['total_spent'].quantile(0.8)
            spending_60th = customer_df['total_spent'].quantile(0.6)
            orders_80th = customer_df['total_orders'].quantile(0.8)
            
            # Gold customers: high spending AND high order frequency
            gold_mask = (customer_df['total_spent'] >= spending_80th) & (customer_df['total_orders'] >= orders_80th)
            customer_df.loc[gold_mask, 'customer_segment'] = 'Gold'
            
            # Silver customers: medium-high spending OR medium-high order frequency
            silver_mask = (customer_df['total_spent'] >= spending_60th) | (customer_df['total_orders'] >= orders_80th)
            customer_df.loc[silver_mask & ~gold_mask, 'customer_segment'] = 'Silver'
        
        # Calculate customer lifetime value (CLV) estimate
        customer_df['estimated_clv'] = customer_df['total_spent'] * 2  # Simple CLV calculation
        
        # Add recency score (days since last order)
        current_date = datetime.now()
        customer_df['last_order_date'] = pd.to_datetime(customer_df['last_order_date'])
        customer_df['days_since_last_order'] = (current_date - customer_df['last_order_date']).dt.days
        customer_df['days_since_last_order'] = customer_df['days_since_last_order'].fillna(999)  # No orders = 999 days
        
        # Recency segments
        customer_df['recency_segment'] = 'Inactive'
        customer_df.loc[customer_df['days_since_last_order'] <= 30, 'recency_segment'] = 'Active'
        customer_df.loc[customer_df['days_since_last_order'] <= 7, 'recency_segment'] = 'Very Active'
        
        self.logger.info(f"Transformed customer data with segments")
        return customer_df
    
    def transform_product_insights(self, product_df: pd.DataFrame) -> pd.DataFrame:
        """Transform product data to create insights"""
        if product_df.empty:
            return product_df
        
        # Calculate performance metrics
        product_df['revenue_per_unit'] = product_df['total_revenue'] / product_df['total_sold'].replace(0, 1)
        product_df['turnover_rate'] = product_df['total_sold'] / product_df['stock_quantity'].replace(0, 1)
        
        # Product performance categories
        if len(product_df) > 0:
            revenue_80th = product_df['total_revenue'].quantile(0.8)
            turnover_80th = product_df['turnover_rate'].quantile(0.8)
            
            product_df['performance_category'] = 'Low Performer'
            product_df.loc[product_df['total_revenue'] >= revenue_80th, 'performance_category'] = 'High Revenue'
            product_df.loc[product_df['turnover_rate'] >= turnover_80th, 'performance_category'] = 'Fast Moving'
            product_df.loc[
                (product_df['total_revenue'] >= revenue_80th) & (product_df['turnover_rate'] >= turnover_80th),
                'performance_category'
            ] = 'Star Product'
        
        # Stock status
        product_df['stock_status'] = 'Normal'
        product_df.loc[product_df['stock_quantity'] <= 10, 'stock_status'] = 'Low Stock'
        product_df.loc[product_df['stock_quantity'] <= 0, 'stock_status'] = 'Out of Stock'
        product_df.loc[product_df['stock_quantity'] >= 500, 'stock_status'] = 'Overstock'
        
        self.logger.info(f"Transformed product data with insights")
        return product_df
    
    def load_to_redis(self, data: Dict[str, Any], key_prefix: str, expiry: int = 3600):
        """Load transformed data to Redis"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for data_type, df in data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Convert DataFrame to dictionary
                data_dict = df.to_dict('records')
                redis_key = f"{key_prefix}:{data_type}:{timestamp}"
                
                self.redis.set_data(redis_key, data_dict, expiry)
                self.logger.info(f"Loaded {len(data_dict)} {data_type} records to Redis: {redis_key}")
                
                # Also store the latest version
                latest_key = f"{key_prefix}:{data_type}:latest"
                self.redis.set_data(latest_key, data_dict, expiry)
    
    def generate_batch_summary(self, customer_df: pd.DataFrame, product_df: pd.DataFrame, 
                             order_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for the batch"""
        summary = {
            'batch_timestamp': datetime.now().isoformat(),
            'batch_number': self.batch_count,
            'records_processed': {
                'customers': len(customer_df),
                'products': len(product_df),
                'order_analytics': len(order_df)
            }
        }
        
        if not customer_df.empty:
            summary['customer_insights'] = {
                'total_customers': len(customer_df),
                'segments': customer_df['customer_segment'].value_counts().to_dict(),
                'recency_segments': customer_df['recency_segment'].value_counts().to_dict(),
                'avg_customer_value': float(customer_df['total_spent'].mean()),
                'total_customer_value': float(customer_df['total_spent'].sum())
            }
        
        if not product_df.empty:
            summary['product_insights'] = {
                'total_products': len(product_df),
                'performance_categories': product_df['performance_category'].value_counts().to_dict(),
                'stock_status': product_df['stock_status'].value_counts().to_dict(),
                'total_revenue': float(product_df['total_revenue'].sum()),
                'avg_units_sold': float(product_df['total_sold'].mean())
            }
        
        if not order_df.empty:
            summary['order_insights'] = {
                'total_orders': int(order_df['order_count'].sum()),
                'total_revenue': float(order_df['total_revenue'].sum()),
                'avg_order_value': float(order_df['avg_order_value'].mean()),
                'status_distribution': order_df.groupby('status')['order_count'].sum().to_dict()
            }
        
        return summary
    
    def run_batch_job(self):
        """Execute a complete batch ETL job"""
        self.batch_count += 1
        start_time = datetime.now()
        
        self.logger.info(f"Starting batch job #{self.batch_count}")
        
        try:
            # Define date range for this batch (last 24 hours)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=1)
            
            # Extract phase
            self.logger.info("Starting EXTRACT phase...")
            customer_data = self.extract_customer_data(start_date, end_date)
            product_data = self.extract_product_performance(start_date, end_date)
            order_data = self.extract_order_analytics(start_date, end_date)
            
            # Transform phase
            self.logger.info("Starting TRANSFORM phase...")
            transformed_customers = self.transform_customer_segments(customer_data)
            transformed_products = self.transform_product_insights(product_data)
            # Order data is already in analytics format
            
            # Load phase
            self.logger.info("Starting LOAD phase...")
            transformed_data = {
                'customers': transformed_customers,
                'products': transformed_products,
                'orders': order_data
            }
            
            self.load_to_redis(transformed_data, 'batch_etl', expiry=7200)  # 2 hours
            
            # Generate and store summary
            summary = self.generate_batch_summary(transformed_customers, transformed_products, order_data)
            self.redis.set_data(f"batch_summary:{self.batch_count}", summary, expiry=86400)  # 24 hours
            
            # Update metrics
            self.redis.increment_counter("batch_jobs:completed")
            
            duration = (datetime.now() - start_time).total_seconds()
            self.redis.set_data("batch_jobs:last_duration", duration, expiry=86400)
            
            self.logger.info(f"Batch job #{self.batch_count} completed in {duration:.2f} seconds")
            self.logger.info(f"Summary: {summary}")
            
        except Exception as e:
            self.logger.error(f"Batch job #{self.batch_count} failed: {e}")
            self.redis.increment_counter("batch_jobs:failed")
            raise

def main():
    """Main batch processor loop"""
    logger = setup_logging()
    db_config, kafka_config, redis_config, app_config = get_config()
    
    logger.info("Starting batch processor...")
    
    # Setup connections
    db_connection = DatabaseConnection(db_config)
    db_connection.connect()
    redis_manager = RedisManager(redis_config)
    
    processor = BatchProcessor(db_connection, redis_manager, logger)
    
    try:
        while True:
            # Run batch job
            processor.run_batch_job()
            
            # Wait for next batch interval
            wait_time = app_config.batch_interval_seconds
            logger.info(f"Waiting {wait_time} seconds until next batch job...")
            time.sleep(wait_time)
            
    except KeyboardInterrupt:
        logger.info("Batch processor stopped by user")
    except Exception as e:
        logger.error(f"Batch processor error: {e}")
    finally:
        db_connection.close()
        redis_manager.close()
        logger.info("Batch processor shut down")

if __name__ == "__main__":
    main()