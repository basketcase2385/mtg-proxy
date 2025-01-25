import os
import psycopg2
import requests
from fastapi import FastAPI, Query
from urllib.parse import quote, unquote
from pydantic import BaseModel  # ✅ Ensure this is imported
class PriceRequest(BaseModel):
    card_names: str  # Expecting a single pipe-separated string ("Black Lotus|Mox Ruby")

# ✅ PostgreSQL Database URL (Render)
DATABASE_URL = "postgresql://mtg_database_user:yuy654YGIgOhE1w7jY5Mn2ZZ53K57YNX@dpg-cu9tv73tq21c739akumg-a.oregon-postgres.render.com/mtg_database"

# ✅ Main API URL (Ensure this is correct)
API_SOURCE_URL = "https://mtgapp.ngrok.app"

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

@app.post("/")
def home():
    """Health check endpoint."""
    return {"message": "MTG Proxy API is running with PostgreSQL!"}

@app.post("/fetch_prices/")
def fetch_prices(request_body: PriceRequest):
    """Fetch card prices via the proxy and return them."""
    if not request_body.card_names:
        return {"error": "No card names provided."}

    return fetch_and_store_data(request_body.card_names)

def fetch_and_store_data(card_names: str):
    """Fetch card prices from the main API using POST, store them in PostgreSQL, and return results."""

    api_url = f"{API_SOURCE_URL}/fetch_prices/"  # ✅ Ensure POST is used
    payload = {"card_names": card_names}  # ✅ JSON body for POST request
    headers = {"Content-Type": "application/json"}  # ✅ Required header

    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=120)  # ✅ Send POST request
        print(f"🔍 API Response Status Code: {response.status_code}")

        if response.status_code != 200:
            print(f"⚠️ Failed to fetch data: {response.status_code} - {response.text}")
            return {"error": f"API request failed: {response.status_code}"}

        data = response.json()  # ✅ Store the response
        store_data_in_db(data)  # ✅ Store the data in PostgreSQL

        print("✅ Returning fetched data to requestor.")
        return data  # ✅ Return the fetched data to the caller

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")
        return {"error": str(e)}


def store_data_in_db(data):
    """Insert fetched card prices into PostgreSQL."""
    conn = get_db_connection()
    cursor = conn.cursor()

    for name, details in data.items():
        cursor.execute("""
            INSERT INTO cards (name, set_name, price)
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET 
            set_name = EXCLUDED.set_name,
            price = EXCLUDED.price
        """, (name.strip(), details.get("set", "Unknown Set"), details.get("price", 0.0)))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data successfully stored in PostgreSQL!")

@app.post("/populate-database/")
def populate_database():
    """Fetch all card names from the main API using POST, then request their prices in a single POST request."""
    print("🔍 Fetching all available card names from the API...")

    try:
        # ✅ Step 1: Ensure we send a **POST** request to `/list_all_cards/`
        headers = {"Content-Type": "application/json"}
        response = requests.post(f"{API_SOURCE_URL}/list_all_cards/", headers=headers, timeout=120)  # **POST, not GET**

        if response.status_code != 200:
            print(f"⚠️ Failed to fetch card list: {response.status_code}")
            return {"error": f"API request failed: {response.status_code}"}

        all_card_names = response.json().get("all_cards", "").split("|")  # ✅ Expecting pipe-separated names

        if not all_card_names:
            print("❌ No cards found in the main API response.")
            return {"error": "No cards found in API response."}

        print(f"✅ Retrieved {len(all_card_names)} card names. Sending a single POST request...")

        # ✅ Step 2: Send all card names in a single **POST** request
        json_body = {"card_names": "|".join(all_card_names)}  # ✅ Pipe-separated format
        response = requests.post(f"{API_SOURCE_URL}/fetch_prices/", json=json_body, headers=headers, timeout=120)  # **POST, not GET**

        if response.status_code != 200:
            print(f"⚠️ Failed to fetch card data: {response.status_code}")
            return {"error": f"API request failed: {response.status_code}"}

        print("✅ Database populated successfully!")
        return {"message": "Database populated successfully!"}

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")
        return {"error": str(e)}
