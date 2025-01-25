import os
import psycopg2
import requests
from fastapi import FastAPI, Query, Body
from pydantic import BaseModel
from urllib.parse import quote, unquote

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

@app.get("/")
def home():
    """Health check endpoint."""
    return {"message": "MTG Proxy API is running with PostgreSQL!"}

# ✅ Schema for POST Requests
class PriceRequest(BaseModel):
    card_names: str  # Expecting a single pipe-separated string ("Black Lotus|Mox Ruby")

@app.api_route("/fetch_prices/", methods=["GET", "POST"])
def fetch_prices(
    card_names: str = Query(None, description="Pipe-separated list of card names (|)"),
    request_body: PriceRequest = Body(None)
):
    """Fetch card prices via the proxy and return them (supports GET & POST)."""

    # ✅ Handle GET and POST requests
    if request_body and request_body.card_names:
        decoded_names = unquote(request_body.card_names)  # POST request
    elif card_names:
        decoded_names = unquote(card_names)  # GET request
    else:
        return {"error": "No card names provided."}

    return fetch_and_store_data(decoded_names)

def fetch_and_store_data(card_names: str):
    """Fetch card prices from the main API and store them in PostgreSQL."""

    # ✅ Prepare JSON payload for the request
    json_body = {"card_names": card_names}

    try:
        response = requests.post(f"{API_SOURCE_URL}/fetch_prices/", json=json_body, timeout=120)  # ✅ Now using POST request
        print(f"🔍 API Response Status Code: {response.status_code}")

        if response.status_code != 200:
            print(f"⚠️ Failed to fetch data: {response.status_code} - {response.text}")
            return {"error": f"API request failed: {response.status_code}"}

        store_data_in_db(response.json())

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")

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
    """Fetch all card names from the main API, then request their prices in batches."""
    print("🔍 Fetching all available card names from the API...")

    try:
        # ✅ Step 1: Get all card names from `/list_all_cards/`
        response = requests.get(f"{API_SOURCE_URL}/list_all_cards/", timeout=120)
        
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch card list: {response.status_code}")
            return {"error": f"API request failed: {response.status_code}"}

        all_card_names = response.json().get("all_cards", [])  # ✅ Expecting {"all_cards": ["Black Lotus", "Mox Emerald", ...]}

        if not all_card_names:
            print("❌ No cards found in the main API response.")
            return {"error": "No cards found in API response."}

        print(f"✅ Retrieved {len(all_card_names)} card names. Processing in batches...")

        # ✅ Step 2: Fetch and store data in batches of 50
        batch_size = 50
        for i in range(0, len(all_card_names), batch_size):
            batch = "|".join(all_card_names[i:i + batch_size])  # ✅ Use `|` instead of `,`

            # ✅ Convert to JSON and send POST request
            json_body = {"card_names": batch}
            requests.post(f"{API_SOURCE_URL}/fetch_prices/", json=json_body, timeout=120)

        print("✅ Database populated successfully!")
        return {"message": "Database populated successfully!"}

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")
        return {"error": str(e)}
