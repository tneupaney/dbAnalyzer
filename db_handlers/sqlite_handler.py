import sqlite3
import time
import hashlib
import os
from sqlalchemy import create_engine, inspect, text

# --- Configuration for Sample SQLite Database ---
SAMPLE_NUM_SHARDS = 2
SAMPLE_SHARD_DB_PREFIX = 'sample_shard_'
SAMPLE_YEARS = [2023, 2024] # Years for sample data generation

# --- Database Setup for Generic Sample SQLite Simulation ---
def setup_sample_database():
    """
    Sets up multiple SQLite databases to simulate sharding, with each shard
    containing a more generic set of tables.
    Includes customers, products, users, orders (potentially partitioned), and audit_log.
    """
    print(f"Setting up {SAMPLE_NUM_SHARDS} sample SQLite shards...")

    # Clean up previous sample shard files
    for i in range(SAMPLE_NUM_SHARDS):
        db_file = f"{SAMPLE_SHARD_DB_PREFIX}{i+1}.db"
        if os.path.exists(db_file):
            os.remove(db_file)

    for shard_id in range(1, SAMPLE_NUM_SHARDS + 1):
        db_file = f"{SAMPLE_SHARD_DB_PREFIX}{shard_id}.db"
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Temporarily disable foreign keys for the entire data insertion block
        cursor.execute(get_fk_check_off_sql())

        # --- Core Tables ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                address TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                product_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                category TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL, -- Storing hashed passwords
                email TEXT,
                last_login TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                entity_type TEXT,
                entity_id INTEGER,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # --- Orders Table (will be populated with mixed dates to simulate partitioning) ---
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date TEXT NOT NULL, --YYYY-MM-DD format
                amount REAL NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        ''')
        # Add a trigger to the orders table for performance testing
        trigger_name = f'after_insert_orders_trigger'
        cursor.execute(f'''
            CREATE TRIGGER IF NOT EXISTS {trigger_name}
            AFTER INSERT ON orders
            FOR EACH ROW
            BEGIN
                INSERT INTO audit_log (action, entity_type, entity_id)
                VALUES ('new_order_inserted', 'order', NEW.order_id);
            END;
        ''')
        print(f"  - Created trigger '{trigger_name}' on 'orders' table in {db_file}")

        # --- Order Items Table ---
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS order_items (
                item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER,
                product_id INTEGER,
                quantity INTEGER NOT NULL,
                item_amount REAL NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        ''')


        # --- Insert Sample Data ---
        # Customers
        cursor.executemany('''
            INSERT OR REPLACE INTO customers (customer_id, name, email, address)
            VALUES (?, ?, ?, ?)
        ''', [
            (1, 'Alice Smith', 'alice@example.com', '123 Main St'),
            (2, 'Bob Johnson', 'bob@example.com', '456 Oak Ave'),
            (3, 'Charlie Brown', 'charlie@example.com', '789 Pine Ln'),
            (4, 'David Lee', 'david@example.com', '101 Elm St'),
            (5, 'Eve Davis', 'eve@example.com', '202 Maple Dr'),
            (6, 'Frank White', 'frank@example.com', '303 Birch Rd')
        ])

        # Products
        cursor.executemany('''
            INSERT OR REPLACE INTO products (product_id, name, price, category)
            VALUES (?, ?, ?, ?)
        ''', [
            (101, 'Laptop Pro', 1500.00, 'Electronics'),
            (102, 'Wireless Mouse', 30.00, 'Accessories'),
            (103, 'Mechanical Keyboard', 120.00, 'Accessories'),
            (104, '4K Monitor', 450.00, 'Electronics'),
            (105, 'USB-C Hub', 50.00, 'Peripherals')
        ])

        # Users (with hashed passwords and one plaintext for detection)
        cursor.executemany('''
            INSERT OR REPLACE INTO users (user_id, username, password_hash, email)
            VALUES (?, ?, ?, ?)
        ''', [
            (1, 'admin_user', hashlib.sha256(b'supersecurepassword!').hexdigest(), 'admin@example.com'),
            (2, 'test_user', hashlib.sha256(b'weakpass').hexdigest(), 'test@example.com'), # Weak password
            (3, 'dev_user', 'plaintext_password_123', 'dev@example.com') # Plaintext for detection
        ])

        # Orders (distributed across shards and years)
        # Distribute orders based on customer_id for sharding simulation
        shard_customer_map = {
            1: [1, 3, 5], # Customers for shard 1
            2: [2, 4, 6]     # Customers for shard 2
        }

        orders_data = []
        for year in SAMPLE_YEARS:
            for month in range(1, 13):
                for day in [1, 15, 28]: # Sample days
                    for customer_id in shard_customer_map[shard_id]:
                        order_date = f'{year}-{month:02d}-{day:02d}'
                        order_id = int(f"{year}{month:02d}{day:02d}{customer_id}{shard_id}")
                        amount = round(100.0 + (customer_id * 10) + (year - 2023) * 50 + (month * 2), 2)
                        orders_data.append((order_id, customer_id, order_date, amount))
        
        # Add one orphaned order for FK violation test
        if shard_id == 1:
            orders_data.append((99999999, 999, '2024-01-01', 100.0)) # Orphaned order

        cursor.executemany('''
            INSERT OR REPLACE INTO orders (order_id, customer_id, order_date, amount)
            VALUES (?, ?, ?, ?)
        ''', orders_data)

        # Order Items (for a subset of orders)
        order_items_data = []
        # Link order items to some existing orders
        if shard_id == 1:
            order_items_data.extend([
                (101001, 101, 1, 1200.00), # order_id, product_id, quantity, item_amount
                (101001, 102, 2, 60.00),
                (201003, 103, 1, 120.00)
            ])
        elif shard_id == 2:
            order_items_data.extend([
                (202002, 104, 1, 450.00),
                (202002, 105, 3, 150.00)
            ])
        
        # Add one orphaned order item (invalid order_id)
        if shard_id == 1:
            order_items_data.append((99999998, 101, 1, 100.0)) # Orphaned order item

        cursor.executemany('''
            INSERT OR REPLACE INTO order_items (order_id, product_id, quantity, item_amount)
            VALUES (?, ?, ?, ?)
        ''', order_items_data)


        # Re-enable foreign keys before committing for this shard
        cursor.execute(get_fk_check_on_sql())
        conn.commit()
        conn.close()
        print(f"  - Shard {shard_id} ({db_file}) setup complete.")
    print("Large database simulation setup complete.")

# --- Helper to get all database connections ---
def get_all_shard_engines(db_paths=None):
    """
    Returns a dictionary of SQLAlchemy engines for all simulated SQLite shards.
    If db_paths is provided, uses those paths. Otherwise, uses default sample shard names.
    """
    engines = {}
    if db_paths:
        for i, db_file in enumerate(db_paths):
            if not os.path.exists(db_file):
                print(f"Warning: Database file '{db_file}' not found. Skipping this shard.")
                continue
            engines[f'shard_{i+1}'] = create_engine(f'sqlite:///{db_file}')
    else:
        for i in range(1, SAMPLE_NUM_SHARDS + 1):
            db_file = f"{SAMPLE_SHARD_DB_PREFIX}{i}.db"
            if not os.path.exists(db_file):
                print(f"Warning: Sample database file '{db_file}' not found. Please run setup_sample_database() first.")
                continue
            engines[f'shard_{i}'] = create_engine(f'sqlite:///{db_file}')
    return engines

# --- Database-specific SQL commands/keywords ---
def get_trigger_query_sql():
    """Returns the SQL query to get trigger information for SQLite."""
    return "SELECT name, sql FROM sqlite_master WHERE type='trigger';"

def get_fk_check_on_sql():
    """Returns the SQL command to enable foreign key checks for SQLite."""
    return "PRAGMA foreign_keys = ON;"

def get_fk_check_off_sql():
    """Returns the SQL command to disable foreign key checks for SQLite."""
    return "PRAGMA foreign_keys = OFF;"

def get_autoincrement_keyword():
    """Returns the auto-increment keyword for SQLite."""
    return "AUTOINCREMENT"

def get_explain_query_plan_prefix():
    """Returns the EXPLAIN query plan prefix for SQLite."""
    return "EXPLAIN QUERY PLAN"
