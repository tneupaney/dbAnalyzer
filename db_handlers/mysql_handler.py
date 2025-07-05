import os
from sqlalchemy import create_engine, inspect, text
import pymysql # You will need to install this: pip install pymysql

# --- Configuration for MySQL Connection ---
# These will be populated by user input in main.py
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = '' # Recommend using environment variables or secure input for real apps
MYSQL_DB_NAME = 'sample_db_shard_'

# --- MySQL Specific Functions ---
def setup_sample_database():
    """
    For MySQL, this function will provide instructions on how to set up
    a sample database manually, as direct programmatic setup requires
    a running MySQL server and credentials.
    """
    print("\n--- MySQL Sample Database Setup ---")
    print("To run analysis on a MySQL sample database, you need a running MySQL server.")
    print("Please ensure you have a MySQL server accessible and create a database/schema.")
    print("The analysis will then connect to this existing database.")
    print("For a sample schema similar to SQLite, you would typically run SQL like:")
    print('''
    CREATE DATABASE IF NOT EXISTS sample_db_shard_1;
    USE sample_db_shard_1;
    CREATE TABLE customers (customer_id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE, address TEXT);
    CREATE TABLE products (product_id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL, price DECIMAL(10,2) NOT NULL, category VARCHAR(255));
    CREATE TABLE users (user_id INT PRIMARY KEY AUTO_INCREMENT, username VARCHAR(255) UNIQUE NOT NULL, password_hash VARCHAR(255) NOT NULL, email VARCHAR(255), last_login DATETIME DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE audit_log (log_id INT PRIMARY KEY AUTO_INCREMENT, action VARCHAR(255) NOT NULL, entity_type VARCHAR(255), entity_id INT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE orders (order_id INT PRIMARY KEY AUTO_INCREMENT, customer_id INT, order_date DATE NOT NULL, amount DECIMAL(10,2) NOT NULL, FOREIGN KEY (customer_id) REFERENCES customers(customer_id));
    CREATE TABLE order_items (item_id INT PRIMARY KEY AUTO_INCREMENT, order_id INT, product_id INT, quantity INT NOT NULL, item_amount DECIMAL(10,2) NOT NULL, FOREIGN KEY (order_id) REFERENCES orders(order_id), FOREIGN KEY (product_id) REFERENCES products(product_id));
    -- For triggers, you'd define them like:
    -- DELIMITER //
    -- CREATE TRIGGER after_insert_orders_trigger AFTER INSERT ON orders
    -- FOR EACH ROW
    -- BEGIN
    --    INSERT INTO audit_log (action, entity_type, entity_id) VALUES ('new_order_inserted', 'order', NEW.order_id);
    -- END;
    -- //
    -- DELIMITER ;
    ''')
    print("You will be prompted for connection details (host, port, user, password, database name) later.")

def get_all_shard_engines(db_connection_details):
    """
    Returns a dictionary of SQLAlchemy engines for MySQL shards.
    db_connection_details is expected to be a dictionary or list of dicts
    containing 'host', 'port', 'user', 'password', 'db_name'.
    """
    engines = {}
    if isinstance(db_connection_details, dict): # Single database
        details = db_connection_details
        try:
            conn_str = f"mysql+pymysql://{details['user']}:{details['password']}@{details['host']}:{details['port']}/{details['db_name']}"
            engines[f'mysql_db_{details["db_name"]}'] = create_engine(conn_str)
            # Test connection
            with engines[f'mysql_db_{details["db_name"]}'].connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"  - Connected to MySQL database: {details['db_name']}")
        except Exception as e:
            print(f"Error connecting to MySQL database {details['db_name']}: {e}")
            return {}
    elif isinstance(db_connection_details, list): # Multiple shards/databases
        for i, details in enumerate(db_connection_details):
            try:
                conn_str = f"mysql+pymysql://{details['user']}:{details['password']}@{details['host']}:{details['port']}/{details['db_name']}"
                engines[f'mysql_shard_{i+1}'] = create_engine(conn_str)
                # Test connection
                with engines[f'mysql_shard_{i+1}'].connect() as conn:
                    conn.execute(text("SELECT 1"))
                print(f"  - Connected to MySQL shard {i+1}: {details['db_name']}")
            except Exception as e:
                print(f"Error connecting to MySQL shard {i+1} ({details['db_name']}): {e}")
                continue
    else:
        print("Invalid connection details provided for MySQL.")
    return engines

# --- Database-specific SQL commands/keywords for MySQL ---
def get_trigger_query_sql():
    """Returns the SQL query to get trigger information for MySQL."""
    return "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = DATABASE();"

def get_fk_check_on_sql():
    """Returns the SQL command to enable foreign key checks for MySQL."""
    return "SET FOREIGN_KEY_CHECKS = 1;"

def get_fk_check_off_sql():
    """Returns the SQL command to disable foreign key checks for MySQL."""
    return "SET FOREIGN_KEY_CHECKS = 0;"

def get_autoincrement_keyword():
    """Returns the auto-increment keyword for MySQL."""
    return "AUTO_INCREMENT"

def get_explain_query_plan_prefix():
    """Returns the EXPLAIN query plan prefix for MySQL."""
    return "EXPLAIN" # MySQL's EXPLAIN output format is different, but this is the keyword
