import os
import psycopg2
import requests
import threading
import time
from fastapi import FastAPI, Query
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extras import execute_values

# ✅ PostgreSQL Database URL (Render)
DATABASE_URL = "postgresql://mtg_database_user:yuy654YGIgOhE1w7jY5Mn2ZZ53K57YNX@dpg-cu9tv73tq21c739akumg-a.oregon-postgres.render.com/mtg_database"

# ✅ Main API URL (Ensure this is correct)
API_SOURCE_URL = "https://mtgapp.ngrok.app/fetch_prices/"

app = FastAPI()

# ✅ Database Connection Function
def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Database Connection Error: {e}")
        raise

# ✅ Ensure the database table exists
def create_table():
    """Create the 'cards' table if it doesn't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cards (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE,
            set_name TEXT,
            price REAL
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

create_table()

@app.get("/")
def home():
    """Health check endpoint."""
    return {"message": "MTG Proxy API is running with PostgreSQL!"}

@app.post("/populate-database/")
def populate_database():
    """Fetch all card names from the main API, then request their prices as fast as possible."""
    print("🔍 Fetching all available card names from the API...")

    try:
        # ✅ Step 1: Fetch the full card list from the main API
        response = requests.get(f"{API_SOURCE_URL}?list_all_cards=true", timeout=120)
        
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch card list: {response.status_code}")
            return {"error": f"API request failed: {response.status_code}"}

        all_card_names = response.json().get("all_cards", [])

        if not all_card_names:
            print("❌ No cards found in the main API response.")
            return {"error": "No cards found in API response."}

        print(f"✅ Retrieved {len(all_card_names)} card names. Processing in parallel batches...")

        # ✅ Step 2: Process in parallel batches using ThreadPoolExecutor
        batch_size = 200  # **Increased Batch Size**
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(fetch_and_store_data, ",".join(all_card_names[i:i + batch_size]))
                for i in range(0, len(all_card_names), batch_size)
            ]

        # ✅ Wait for all tasks to complete
        for future in futures:
            future.result()

        print("✅ Database populated successfully!")
        return {"message": "Database populated successfully!"}

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")
        return {"error": str(e)}

def fetch_and_store_data(card_names: str):
    """Fetch card prices in parallel from the main API and store them in PostgreSQL."""
    encoded_names = quote(card_names, safe=",")
    api_url = f"{API_SOURCE_URL}?card_names={encoded_names}"

    try:
        response = requests.get(api_url, timeout=120)
        print(f"🔍 API Response Status Code: {response.status_code}")

        if response.status_code != 200:
            print(f"⚠️ Failed to fetch data: {response.status_code} - {response.text}")
            return {"error": f"API request failed: {response.status_code}"}

        data = response.json()
        store_data_in_db(data)

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")

def store_data_in_db(data):
    """Insert fetched card prices into PostgreSQL using bulk inserts for speed."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # ✅ Use Bulk Insert for Maximum Speed
    insert_data = [
        (name.strip(), details.get("set", "Unknown Set"), details.get("price", 0.0))
        for name, details in data.items()
    ]

    query = """
        INSERT INTO cards (name, set_name, price)
        VALUES %s
        ON CONFLICT (name) DO UPDATE SET 
        set_name = EXCLUDED.set_name,
        price = EXCLUDED.price
    """

    execute_values(cursor, query, insert_data)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Stored {len(insert_data)} cards in PostgreSQL!")

@app.post("/update-database/")
def update_database():
    """Fetch updated data for all cards and store it in PostgreSQL."""
    return populate_database()

# ✅ **Automatic Database Update & Corruption Check**
def auto_update_database():
    """Runs every 24 hours to update prices & repopulate if necessary."""
    while True:
        print("🕒 Running Scheduled Database Check...")
        
        # ✅ Step 1: Check if Database is Empty or Corrupt
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM cards")
        count = cursor.fetchone()[0]
        conn.close()

        if count == 0:
            print("⚠️ Database is empty! Repopulating...")
            populate_database()
        else:
            print(f"✅ Database contains {count} records. Running an update...")
            update_database()

        # ✅ Wait 24 Hours Before Running Again
        time.sleep(86400)  # **Wait for 24 hours**

# ✅ **Start Background Auto-Updater**
auto_update_thread = threading.Thread(target=auto_update_database, daemon=True)
auto_update_thread.start()
