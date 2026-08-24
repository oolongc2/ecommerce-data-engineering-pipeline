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
            DROP TABLE IF EXISTS USERS CASCADE;
            """)

cur.execute("""
            DROP TABLE IF EXISTS PRODUCTS CASCADE;
            """)

cur.execute("""
            DROP TABLE IF EXISTS ORDERS CASCADE;
            """)