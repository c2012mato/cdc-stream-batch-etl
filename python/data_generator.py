"""
Mock data generator for the ETL system
"""
import time
import random
from faker import Faker
from typing import Dict, Any
import logging
from utils import DatabaseConnection, setup_logging
from config import get_config

fake = Faker()

class DataGenerator:
    """Generate mock data for testing the ETL pipeline"""
    
    def __init__(self, db_connection: DatabaseConnection, logger: logging.Logger):
        self.db = db_connection
        self.logger = logger
    
    def generate_customer(self) -> Dict[str, Any]:
        """Generate a random customer record"""
        return {
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.unique.email(),
            'phone': fake.phone_number()[:20],  # Ensure phone fits in VARCHAR(20)
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode()
        }
    
    def generate_product(self) -> Dict[str, Any]:
        """Generate a random product record"""
        categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys']
        return {
            'name': fake.catch_phrase(),
            'description': fake.text(max_nb_chars=200),
            'price': round(random.uniform(5.00, 999.99), 2),
            'category': random.choice(categories),
            'stock_quantity': random.randint(0, 1000)
        }
    
    def insert_customer(self, customer: Dict[str, Any]) -> int:
        """Insert a customer into the database"""
        query = """
        INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code)
        VALUES (%(first_name)s, %(last_name)s, %(email)s, %(phone)s, %(address)s, %(city)s, %(state)s, %(zip_code)s)
        RETURNING id
        """
        try:
            return self.db.execute_insert(query, customer)
        except Exception as e:
            self.logger.error(f"Failed to insert customer: {e}")
            return None
    
    def insert_product(self, product: Dict[str, Any]) -> int:
        """Insert a product into the database"""
        query = """
        INSERT INTO products (name, description, price, category, stock_quantity)
        VALUES (%(name)s, %(description)s, %(price)s, %(category)s, %(stock_quantity)s)
        RETURNING id
        """
        try:
            return self.db.execute_insert(query, product)
        except Exception as e:
            self.logger.error(f"Failed to insert product: {e}")
            return None
    
    def generate_order(self, customer_ids: list, product_ids: list) -> Dict[str, Any]:
        """Generate a random order with order items"""
        if not customer_ids or not product_ids:
            return None
        
        customer_id = random.choice(customer_ids)
        num_items = random.randint(1, 5)
        selected_products = random.sample(product_ids, min(num_items, len(product_ids)))
        
        # Calculate total amount
        total_amount = 0
        order_items = []
        
        for product_id in selected_products:
            # Get product price (simplified - in real scenario, we'd fetch from DB)
            unit_price = round(random.uniform(10.00, 200.00), 2)
            quantity = random.randint(1, 3)
            total_amount += unit_price * quantity
            
            order_items.append({
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price
            })
        
        return {
            'customer_id': customer_id,
            'total_amount': round(total_amount, 2),
            'status': random.choice(['pending', 'processing', 'shipped', 'delivered']),
            'shipping_address': fake.address(),
            'order_items': order_items
        }
    
    def insert_order(self, order: Dict[str, Any]) -> int:
        """Insert an order and its items into the database"""
        # Insert order
        order_query = """
        INSERT INTO orders (customer_id, total_amount, status, shipping_address)
        VALUES (%(customer_id)s, %(total_amount)s, %(status)s, %(shipping_address)s)
        RETURNING id
        """
        
        try:
            order_id = self.db.execute_insert(order_query, {
                'customer_id': order['customer_id'],
                'total_amount': order['total_amount'],
                'status': order['status'],
                'shipping_address': order['shipping_address']
            })
            
            if order_id and order.get('order_items'):
                # Insert order items
                for item in order['order_items']:
                    item_query = """
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                    VALUES (%s, %s, %s, %s)
                    """
                    self.db.execute_insert(item_query, (
                        order_id, item['product_id'], item['quantity'], item['unit_price']
                    ))
            
            return order_id
        except Exception as e:
            self.logger.error(f"Failed to insert order: {e}")
            return None
    
    def get_existing_ids(self) -> Dict[str, list]:
        """Get existing customer and product IDs for generating orders"""
        try:
            customers = self.db.execute_query("SELECT id FROM customers")
            products = self.db.execute_query("SELECT id FROM products")
            
            return {
                'customer_ids': [row['id'] for row in customers],
                'product_ids': [row['id'] for row in products]
            }
        except Exception as e:
            self.logger.error(f"Failed to get existing IDs: {e}")
            return {'customer_ids': [], 'product_ids': []}
    
    def update_random_record(self):
        """Update a random existing record to trigger CDC"""
        try:
            # Randomly choose to update a customer or product
            if random.choice([True, False]):
                # Update customer
                customers = self.db.execute_query("SELECT id FROM customers ORDER BY RANDOM() LIMIT 1")
                if customers:
                    customer_id = customers[0]['id']
                    new_phone = fake.phone_number()[:20]
                    query = "UPDATE customers SET phone = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s"
                    self.db.execute_insert(query, (new_phone, customer_id))
                    self.logger.info(f"Updated customer {customer_id} phone number")
            else:
                # Update product stock
                products = self.db.execute_query("SELECT id FROM products ORDER BY RANDOM() LIMIT 1")
                if products:
                    product_id = products[0]['id']
                    new_stock = random.randint(0, 1000)
                    query = "UPDATE products SET stock_quantity = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s"
                    self.db.execute_insert(query, (new_stock, product_id))
                    self.logger.info(f"Updated product {product_id} stock to {new_stock}")
        except Exception as e:
            self.logger.error(f"Failed to update random record: {e}")

def main():
    """Main data generation loop"""
    logger = setup_logging()
    db_config, kafka_config, redis_config, app_config = get_config()
    
    logger.info("Starting data generator...")
    
    # Setup database connection
    db_connection = DatabaseConnection(db_config)
    db_connection.connect()
    
    generator = DataGenerator(db_connection, logger)
    
    try:
        while True:
            # Generate batch of records
            batch_size = app_config.mock_data_records_per_batch
            
            logger.info(f"Generating batch of {batch_size} records...")
            
            # Generate customers and products
            for i in range(batch_size // 4):  # 25% customers
                customer = generator.generate_customer()
                customer_id = generator.insert_customer(customer)
                if customer_id:
                    logger.debug(f"Created customer {customer_id}")
            
            for i in range(batch_size // 4):  # 25% products
                product = generator.generate_product()
                product_id = generator.insert_product(product)
                if product_id:
                    logger.debug(f"Created product {product_id}")
            
            # Generate orders (50% of batch)
            existing_ids = generator.get_existing_ids()
            for i in range(batch_size // 2):
                order = generator.generate_order(
                    existing_ids['customer_ids'], 
                    existing_ids['product_ids']
                )
                if order:
                    order_id = generator.insert_order(order)
                    if order_id:
                        logger.debug(f"Created order {order_id}")
            
            # Perform some random updates to trigger CDC
            for _ in range(5):
                generator.update_random_record()
            
            logger.info(f"Completed batch generation. Waiting {app_config.mock_data_interval_seconds} seconds...")
            time.sleep(app_config.mock_data_interval_seconds)
            
    except KeyboardInterrupt:
        logger.info("Data generator stopped by user")
    except Exception as e:
        logger.error(f"Data generator error: {e}")
    finally:
        db_connection.close()
        logger.info("Data generator shut down")

if __name__ == "__main__":
    main()