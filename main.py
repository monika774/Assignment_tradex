import sqlite3
import threading
import re
from dataclasses import dataclass
from typing import List
from queue import Queue
import time

# Models Created for User
@dataclass
class User:
    id: int
    name: str
    email: str
    
    def is_valid(self) -> tuple[bool, str]:
        if not self.name:
            return False, "Name can't be empty"
        if not self.email:
            return False, "Email can't be empty"
        if not re.match(r"[^@]+@[^@]+\.[^@]+", self.email):
            return False, "Invalid format of mail"
        return True, ""

# Models created for Product
@dataclass
class Product:
    id: int
    name: str
    price: float
    
    def is_valid(self) -> tuple[bool, str]:
        if not self.name:
            return False, "Name can't be empty"
        if self.price <= 0:
            return False, "Please enter positive price"
        return True, ""

# Models created for Order
@dataclass
class Order:
    id: int
    user_id: int
    product_id: int
    quantity: int
    
    def is_valid(self) -> tuple[bool, str]:
        if self.quantity <= 0:
            return False, "Please enter Quantity of Product positive"
        return True, ""

# Database Operations
class DatabaseManager:
    def __init__(self):
        self.results_queue = Queue()
        
        # Creating  databases and tables
        self.create_databases()
    
    def create_databases(self):
        # Connecting to the  users database
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS users
                    (id INTEGER PRIMARY KEY, name TEXT, email TEXT)''')
        conn.commit()
        conn.close()
        
        # Connecting to the  products database
        conn = sqlite3.connect('products.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS products
                    (id INTEGER PRIMARY KEY, name TEXT, price REAL)''')
        conn.commit()
        conn.close()
        
        # Connecting to the orders database
        conn = sqlite3.connect('orders.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS orders
                    (id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, quantity INTEGER)''')
        conn.commit()
        conn.close()

    def insert_user(self, user: User):
        try:
            is_valid, error_msg = user.is_valid()
            if not is_valid:
                self.results_queue.put(f"User {user.id}: Validation failed - {error_msg}")
                return

            conn = sqlite3.connect('users.db')
            c = conn.cursor()
            c.execute("INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
                     (user.id, user.name, user.email))
            conn.commit()
            conn.close()
            self.results_queue.put(f"User {user.id}: Successfully inserted")
        except sqlite3.IntegrityError:
            self.results_queue.put(f"User {user.id}: Failed - Duplicate ID")
        except Exception as e:
            self.results_queue.put(f"User {user.id}: Failed - {str(e)}")

    def insert_product(self, product: Product):
        try:
            is_valid, error_msg = product.is_valid()
            if not is_valid:
                self.results_queue.put(f"Product {product.id}: Validation failed - {error_msg}")
                return

            conn = sqlite3.connect('products.db')
            c = conn.cursor()
            c.execute("INSERT INTO products (id, name, price) VALUES (?, ?, ?)",
                     (product.id, product.name, product.price))
            conn.commit()
            conn.close()
            self.results_queue.put(f"Product {product.id}: Successfully inserted")
        except sqlite3.IntegrityError:
            self.results_queue.put(f"Product {product.id}: Failed - Duplicate ID")
        except Exception as e:
            self.results_queue.put(f"Product {product.id}: Failed - {str(e)}")

    def insert_order(self, order: Order):
        try:
            is_valid, error_msg = order.is_valid()
            if not is_valid:
                self.results_queue.put(f"Order {order.id}: Validation failed - {error_msg}")
                return

            conn = sqlite3.connect('orders.db')
            c = conn.cursor()
            c.execute("INSERT INTO orders (id, user_id, product_id, quantity) VALUES (?, ?, ?, ?)",
                     (order.id, order.user_id, order.product_id, order.quantity))
            conn.commit()
            conn.close()
            self.results_queue.put(f"Order {order.id}: Successfully inserted")
        except sqlite3.IntegrityError:
            self.results_queue.put(f"Order {order.id}: Failed - Duplicate ID")
        except Exception as e:
            self.results_queue.put(f"Order {order.id}: Failed - {str(e)}")

def main():
    # Sample  of data
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
        User(10, "", "jane@example.com"),
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

    # Initialize database manager
    db_manager = DatabaseManager()

    # Create threads for each insertion
    threads = []
    
    # User threads
    for user in users:
        thread = threading.Thread(target=db_manager.insert_user, args=(user,))
        threads.append(thread)
    
    # Product threads
    for product in products:
        thread = threading.Thread(target=db_manager.insert_product, args=(product,))
        threads.append(thread)
    
    # Order threads
    for order in orders:
        thread = threading.Thread(target=db_manager.insert_order, args=(order,))
        threads.append(thread)

    # Start all threads
    for thread in threads:
        thread.start()

    # Waiting thread to complete all threads 
    for thread in threads:
        thread.join()

    # Print results
    print("\nInsertion Results:")
    print("=" * 50)
    while not db_manager.results_queue.empty():
        print(db_manager.results_queue.get())

if __name__ == "__main__":
    main()