import os
import time
import csv
import pandas as pd
from faker import Faker
from dotenv import load_dotenv
import psycopg2

# Load environment variables
load_dotenv()

# Database credentials from env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

TOTAL_RECORDS = 500_000
CHUNK_SIZE = 100_000  # Write CSV in batches

# Step 1: Generate 500,000 fake customer records
def generate_csv(Customers, num_records=500_000):
    fake = Faker()
    with open(Customers, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id', 'name', 'email', 'address', 'created_at'])
        for i in range(1, num_records + 1):
            writer.writerow([
                i,
                fake.name(),
                fake.email(),
                fake.address().replace('\n', ', '),
                fake.date_time_this_decade()
            ])

# Step 2: Load CSV into PostgreSQL using COPY for performance
def load_csv_to_db(customers):
    start_time = time.time()
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            address TEXT,
            created_at TIMESTAMP
        );
    """)
    conn.commit()

    # Use COPY for fast import
    with open(customers, 'r') as f:
        next(f)  # Skip header
        cursor.copy_expert("COPY customers FROM STDIN WITH CSV", f)
    conn.commit()
    cursor.close()
    conn.close()
    end_time = time.time()
    print(f"CSV loaded in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    filename = "customers.csv"
    print("Generating CSV file...")
    generate_csv(filename)
    print("CSV file generated.")
    print("Loading CSV into database...")
    load_csv_to_db(filename)
