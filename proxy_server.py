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
        response = requests.post(api_url, json=payload, headers=headers, timeout=300)  # ✅ Send POST request
        print(f"🔍 API Response Status Code: {response.status_code}")

        if response.status_code != 200:
            print(f"⚠️ Failed to fetch data: {response.status_code} - {response.text}")
            return {"error": f"API request failed: {response.status_code}"}

        data = response.json()  # ✅ Store the response
        
        # ✅ Log fetched data before storing
        print("📥 Fetched Prices Data:")
        for name, details in data.items():
            print(f"   - {name}: {details}")

        store_data_in_db(data)  # ✅ Store the data in PostgreSQL

        return data  # ✅ Return the fetched data to the caller

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")
        return {"error": str(e)}

import logging

def store_data_in_db(data):
    """Insert fetched card prices into PostgreSQL, storing only the most recent price."""

    conn = get_db_connection()
    cursor = conn.cursor()

    for uuid, details in data.get("data", {}).items():
        # Extract price data from different sources
        paper_prices = details.get("paper", {}).get("cardkingdom", {}).get("retail", {}).get("normal", {})
        tcg_prices = details.get("tcgplayer", {}).get("retail", {}).get("normal", {})
        cardmarket_prices = details.get("cardmarket", {}).get("retail", {}).get("normal", {})

        # ✅ Debugging Output
        logging.info(f"🔎 UUID: {uuid}")
        logging.info(f"   📄 Paper Prices: {paper_prices}")
        logging.info(f"   🏷️ TCG Prices: {tcg_prices}")
        logging.info(f"   🌍 Cardmarket Prices: {cardmarket_prices}")

        # ✅ Extract the most recent price
        latest_paper_price = paper_prices.get(max(paper_prices.keys(), default=""), 0.0)
        latest_tcg_price = tcg_prices.get(max(tcg_prices.keys(), default=""), 0.0)
        latest_cardmarket_price = cardmarket_prices.get(max(cardmarket_prices.keys(), default=""), 0.0)

        # ✅ Select the best available price (Priority: Paper > TCG > Cardmarket)
        final_price = latest_paper_price or latest_tcg_price or latest_cardmarket_price or 0.0

        logging.info(f"   ✅ Selected Price for {uuid}: ${final_price}")

        try:
            cursor.execute("""
                INSERT INTO cards (name, set_name, price)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET 
                set_name = EXCLUDED.set_name,
                price = EXCLUDED.price
            """, (uuid.strip(), "Unknown Set", final_price))

            logging.info(f"✅ Successfully stored {uuid} with price: ${final_price}")

        except Exception as e:
            logging.error(f"❌ Error storing {uuid}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("✅ All data successfully stored in PostgreSQL!")

@app.post("/populate-database/")
def populate_database():
    """Fetch all card names from the main API, then request their prices in batches."""
    print("🔍 Fetching all available card names from the API...")

    try:
        # ✅ Step 1: Get all card names from `/list_all_cards/`
        response = requests.post(f"{API_SOURCE_URL}/list_all_cards/", timeout=120)
        
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch card list: {response.status_code}")
            return {"error": f"API request failed: {response.status_code}"}

        all_card_names = response.json().get("all_cards", "").split("|")  # ✅ Expecting pipe-separated names

        if not all_card_names:
            print("❌ No cards found in the main API response.")
            return {"error": "No cards found in API response."}

        print(f"✅ Retrieved {len(all_card_names)} card names. Processing in batches...")

        # ✅ Step 2: Fetch and store data in batches of 50
        batch_size = 50
        for i in range(0, len(all_card_names), batch_size):
            batch = "|".join(all_card_names[i:i + batch_size])  # ✅ Create a batch of card names
            print(f"🔄 Processing batch {i // batch_size + 1} of {len(all_card_names) // batch_size + 1}")

            # ✅ Convert to JSON and send POST request
            json_body = {"card_names": batch}
            headers = {"Content-Type": "application/json"}
            response = requests.post(f"{API_SOURCE_URL}/fetch_prices/", json=json_body, headers=headers, timeout=120)

            if response.status_code != 200:
                print(f"⚠️ Batch {i // batch_size + 1} failed: {response.status_code}")
            else:
                print(f"✅ Batch {i // batch_size + 1} processed successfully.")

        print("✅ Database populated successfully!")
        return {"message": "Database populated successfully!"}

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")
        return {"error": str(e)}

@app.get("/get_stored_prices/")
def get_stored_prices():
    """Retrieve all stored card prices from the PostgreSQL database."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # ✅ Fetch all stored prices
    cursor.execute("SELECT name, set_name, price FROM cards;")
    rows = cursor.fetchall()

    conn.close()

    # ✅ If no data is found, return a message
    if not rows:
        return {"message": "No card prices found in the database."}

    # ✅ Convert results to a list of dictionaries
    stored_prices = [{"name": row[0], "set": row[1], "price": row[2]} for row in rows]

    return {"stored_prices": stored_prices}

