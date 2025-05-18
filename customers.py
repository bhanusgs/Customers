import pandas as pd
import psycopg2
from faker import Faker
import csv
import time

# Load environment variables
load_dotenv()

# Database credentials from env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Configurations
CSV_FILE = "customers.csv"
DB_CONFIG = {
    "host": "DB_HOST",
    "dbname": "DB_NAME",
    "user": "DB_USER",
    "password": "DB_PASSWORD",
    "port": DB_PORT
}
TOTAL_RECORDS = 500_000
CHUNK_SIZE = 100_000  # Write CSV in batches

# Define the columns
COLUMNS = [
    "customer_id", "first_name", "last_name", "email", "phone",
    "address", "city", "state", "zip"
]

# Step 1: Generate CSV
def generate_customer_csv(filename=CSV_FILE, total=TOTAL_RECORDS):
    fake = Faker()
    print(f"📄 Generating {total} customer records in '{filename}'...")
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(COLUMNS)  # Write header
        
        for _ in range(total // CHUNK_SIZE):
            rows = [
                [
                    fake.uuid4(),
                    fake.first_name(),
                    fake.last_name(),
                    fake.email(),
                    fake.phone_number(),
                    fake.street_address().replace("\n", " ").replace(",", ""),
                    fake.city(),
                    fake.state_abbr(),
                    fake.zipcode()
                ]
                for _ in range(CHUNK_SIZE)
            ]
            writer.writerows(rows)
    print("✅ CSV generation complete.")

# Step 2: Load CSV into PostgreSQL using COPY
def load_csv_to_postgres(csv_file=CSV_FILE):
    print(f"🛢️  Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("🧹 Dropping and creating table 'customers'...")
    cur.execute("DROP TABLE IF EXISTS customers")
    cur.execute("""
        CREATE TABLE customers (
            customer_id TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT
        )
    """)
    conn.commit()

    print(f"⬆️  Loading data from '{csv_file}' into PostgreSQL...")
    start_time = time.time()

    with open(csv_file, "r", encoding="utf-8") as f:
        cur.copy_expert("COPY customers FROM STDIN WITH CSV HEADER", f)

    conn.commit()
    end_time = time.time()
    print(f"✅ Load complete in {end_time - start_time:.2f} seconds.")

    cur.close()
    conn.close()

# Run the ETL process
if __name__ == "__main__":
    generate_customer_csv()
    load_csv_to_postgres()

