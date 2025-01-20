import sqlite3
import threading
import logging
from dataclasses import dataclass
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor
import re
from tabulate import tabulate
from datetime import datetime

# logging configuration level of the data
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Data model for User
@dataclass
class User:
    id: int
    name: str
    email: str

# Data model for Product
@dataclass
class Product:
    id: int
    name: str
    price: float

# Data model for Order
@dataclass
class Order:
    id: int
    user_id: int
    product_id: int
    quantity: int

class DatabaseManager:
    def __init__(self):
        self.setup_databases()
        self.lock = threading.Lock()
        
    def setup_databases(self):
        # Create users database with the setup sqlite for the user
        with sqlite3.connect('users.db') as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL
                )
            ''')
            
        # CCreate users database with the setup sqlite for the product
        with sqlite3.connect('products.db') as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    price REAL NOT NULL
                )
            ''')
            
        # Create users database with the setup sqlite for the order
        with sqlite3.connect('orders.db') as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL
                )
            ''')

    def validate_user(self, user: User) -> Optional[str]:
        if not user.name or not isinstance(user.name, str):
            return "Invalid name"
        if not user.email or not isinstance(user.email, str):
            return "Invalid email"
        # This is the email validation validation code 
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, user.email):
            return "Invalid email format"
        return None

    def validate_product(self, product: Product) -> Optional[str]:
        if not product.name or not isinstance(product.name, str):
            return "Invalid product name"
        # validation for the product if the product price has negative value
        if not isinstance(product.price, (int, float)) or product.price < 0:
            return "Invalid price - must be non-negative"
        return None

    def validate_order(self, order: Order) -> Optional[str]:
        if order.quantity < 0:
            return "Invalid quantity - must be non-negative"
        # validation code for the if user and product has invalid if
        if order.user_id < 1 or order.product_id < 1:
            return "Invalid user_id or product_id"
        return None

    def insert_user(self, user: User) -> tuple[bool, str]:
        error = self.validate_user(user)
        if error:
            return False, error
        
        try:
            with self.lock, sqlite3.connect('users.db') as conn:
                conn.execute(
                    'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
                    (user.id, user.name, user.email)
                )
                return True, "Success"
        except sqlite3.IntegrityError as e:
            return False, f"Database error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"

    def insert_product(self, product: Product) -> tuple[bool, str]:
        error = self.validate_product(product)
        if error:
            return False, error
        
        try:
            with self.lock, sqlite3.connect('products.db') as conn:
                conn.execute(
                    'INSERT INTO products (id, name, price) VALUES (?, ?, ?)',
                    (product.id, product.name, product.price)
                )
                return True, "Success"
        except sqlite3.IntegrityError as e:
            return False, f"Database error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"

    def insert_order(self, order: Order) -> tuple[bool, str]:
        error = self.validate_order(order)
        if error:
            return False, error
        
        try:
            with self.lock, sqlite3.connect('orders.db') as conn:
                conn.execute(
                    'INSERT INTO orders (id, user_id, product_id, quantity) VALUES (?, ?, ?, ?)',
                    (order.id, order.user_id, order.product_id, order.quantity)
                )
                return True, "Success"
        except sqlite3.IntegrityError as e:
            return False, f"Database error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"
   

    #fetching all the data in the database
    def get_all_data(self):
        users_data = []
        with sqlite3.connect('users.db') as conn:
            cursor = conn.execute('SELECT * FROM users')
            users_data = cursor.fetchall()
            
        products_data = []
        with sqlite3.connect('products.db') as conn:
            cursor = conn.execute('SELECT * FROM products')
            products_data = cursor.fetchall()
            
        orders_data = []
        with sqlite3.connect('orders.db') as conn:
            cursor = conn.execute('SELECT * FROM orders')
            orders_data = cursor.fetchall()
            
        return users_data, products_data, orders_data

def main():
    db_manager = DatabaseManager()
    
    # Sample data
    users = [
        User(1, "Alice", "alice@example.com"),
        User(2, "Bob", "bob@example.com"),
        User(3, "Charlie", "charlie@example.com"),
        User(4, "David", "david@example.com"),
        User(5, "Eve", "eve@example.com"),
        User(6, "Frank", "frank@example.com"),
        User(7, "Grace", "grace@example.com"),
        User(8, "Alice", "alice@example.com"),
        User(9, "Henry", "henry@example.com"),
        User(10, "Jane", "jane@example.com"),
    ]
    
    products = [
        Product(1, "Laptop", 1000.00),
        Product(2, "Smartphone", 700.00),
        Product(3, "Headphones", 150.00),
        Product(4, "Monitor", 300.00),
        Product(5, "Keyboard", 50.00),
        Product(6, "Mouse", 30.00),
        Product(7, "Laptop", 1000.00),
        Product(8, "Smartwatch", 250.00),
        Product(9, "Gaming Chair", 500.00),
        Product(10, "Earbuds", -50.00),
    ]
    
    orders = [
        Order(1, 1, 1, 2),
        Order(2, 2, 2, 1),
        Order(3, 3, 3, 5),
        Order(4, 4, 4, 1),
        Order(5, 5, 5, 3),
        Order(6, 6, 6, 4),
        Order(7, 7, 7, 2),
        Order(8, 8, 8, 0),
        Order(9, 9, 1, -1),
        Order(10, 10, 11, 2),
    ]
    
    # Results storage
    user_results = []
    product_results = []
    order_results = []
    
    # Concurrent insertion using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Insert users
        for user in users:
            success, message = db_manager.insert_user(user)
            user_results.append({
                'id': user.id,
                'success': success,
                'message': message
            })
            
        # Insert products
        for product in products:
            success, message = db_manager.insert_product(product)
            product_results.append({
                'id': product.id,
                'success': success,
                'message': message
            })
            
        # Insert orders
        for order in orders:
            success, message = db_manager.insert_order(order)
            order_results.append({
                'id': order.id,
                'success': success,
                'message': message
            })
    
    # Fetch and display results
    users_data, products_data, orders_data = db_manager.get_all_data()
    
    # Display validation results
    print("\nValidation Data Results:")
    print("\nUsers  Validation Data:")
    users_table = [[r['id'], r['success'], r['message']] for r in user_results]
    print(tabulate(users_table, headers=['ID', 'Success', 'Message'], tablefmt='grid'))
    
    print("\nProducts Validation Data:")
    products_table = [[r['id'], r['success'], r['message']] for r in product_results]
    print(tabulate(products_table, headers=['ID', 'Success', 'Message'], tablefmt='grid'))
    
    print("\nOrders Validation Data:")
    orders_table = [[r['id'], r['success'], r['message']] for r in order_results]
    print(tabulate(orders_table, headers=['ID', 'Success', 'Message'], tablefmt='grid'))
    
    # successfully inserted data
    print("\nSuccessfully Inserted Data in database:")
    print("\nUsers Data Table:")
    print(tabulate(users_data, headers=['ID', 'Name', 'Email'], tablefmt='grid'))
    
    print("\nProducts Data Table:")
    print(tabulate(products_data, headers=['ID', 'Name', 'Price'], tablefmt='grid'))
    
    print("\nOrders  Data Table:")
    print(tabulate(orders_data, headers=['ID', 'User ID', 'Product ID', 'Quantity'], tablefmt='grid'))

if __name__ == "__main__":
    main()