import psycopg2
import random
import time
from faker import Faker

fake = Faker()

# Database connection (PostgreSQL)

conn = psycopg2.connect(
    host="localhost",
    database="ecommerce_db",
    user="postgres",
    password="123456"
)
conn.autocommit = True
cur = conn.cursor()

# Create tables
cur.execute("""
            CREATE TABLE IF NOT EXISTS USERS (
                user_id SERIAL PRIMARY KEY,
                username TEXT,
                email TEXT UNIQUE,
                user_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

cur.execute("""
            CREATE TABLE IF NOT EXISTS PRODUCTS (
                product_id SERIAL PRIMARY KEY,
                product_name TEXT,
                price NUMERIC(10, 2),
                product_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

cur.execute("""
            CREATE TABLE IF NOT EXISTS ORDERS (
                order_id SERIAL PRIMARY KEY,
                user_id INT,
                product_id INT,
                quantity INT,
                total_price NUMERIC(10, 2),
                order_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE constraint_name = 'unique_product_name'
                    AND table_name = 'products'
                ) THEN
                    ALTER TABLE products
                    ADD CONSTRAINT unique_product_name UNIQUE (product_name);
                END IF;
            END $$;
            """
            )

# Seed Initial Data
def seed_data():
    # insert users:
    for _ in range(20):
        cur.execute(
                    "INSERT INTO USERS (username, email) VALUES (%s, %s)",
                    (fake.user_name(), fake.email())
                    )
    
    # insert products:
    products = [
        ("Laptop", 999.99),
        ("Smartphone", 499.99),
        ("Headphones", 199.99),
        ("Smartwatch", 299.99),
        ("Tablet", 399.99)
    ]
    for p in products:
        cur.execute(
                    "INSERT INTO PRODUCTS (product_name, price) VALUES (%s, %s)" \
                    "ON CONFLICT (product_name) DO NOTHING",
                    p
                    )

# Generate random orders
def generate_orders():
    # random user
    cur.execute("SELECT user_id FROM USERS ORDER BY RANDOM() LIMIT 1")
    user_id = cur.fetchone()[0]

    # random product
    cur.execute("SELECT product_id, price FROM PRODUCTS ORDER BY RANDOM() LIMIT 1")
    product_id, price = cur.fetchone()

    quantity = random.randint(1, 5)
    total_price = quantity * price

    # insert order
    cur.execute("""
                INSERT INTO ORDERS (user_id, product_id, quantity, total_price)
                VALUES (%s, %s, %s, %s)""",
                (user_id, product_id, quantity, total_price)
    )

    print(f"[NEW ORDER] User ID: {user_id} bought product {product_id} x {quantity} -> ${total_price}")

# Optionally create new users
def create_user():
    cur.execute(
        "INSERT INTO users (username, email) VALUES (%s, %s)",
        (fake.user_name(), fake.email())
    )
    print("[NEW USER ADDED]")


# Main execution
if __name__ == "__main__":
    print("Seeding initial data...")
    seed_data()
    print("Initial data seeded. Starting to generate real-time orders...")

    try:
        while True:
            generate_orders()
            
            # occasionally add new user
            if random.random() < 0.2:
                create_user()

            time.sleep(random.randint(1, 5))  # Simulate random order intervals
    except KeyboardInterrupt:
        print("Stopping order generation.")
    finally:
        cur.close()
        conn.close()