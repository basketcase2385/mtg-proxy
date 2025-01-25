import os
import psycopg2
import requests
from fastapi import FastAPI, Query
from urllib.parse import quote, unquote

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

@app.get("/card/{card_name}")
def get_card(card_name: str):
    """Fetch card details from PostgreSQL while handling names with commas properly."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # ✅ Use full name without truncation
    cursor.execute("SELECT name, set_name, price FROM cards WHERE name = %s", (card_name,))
    result = cursor.fetchone()
    
    conn.close()

    if result:
        return {"name": result[0], "set": result[1], "price": result[2]}
    
    return {"error": f"Card '{card_name}' not found"}

# ✅ **Add the Missing `/fetch_prices/` Route**
@app.get("/fetch_prices/")
def fetch_prices(card_names: str = Query(..., description="Comma-separated list of card names")):
    """Fetch card prices via the proxy and return them."""
    return fetch_and_store_data(card_names)

# ✅ Fetch & Store Data in Batches to Prevent Overload
def fetch_and_store_data(card_names: str):
    """Fetch card prices from the main API and store them in PostgreSQL."""
    
    # ✅ Encode names properly (fixes comma-related issues)
    encoded_names = quote(card_names, safe=",")  
    api_url = f"{API_SOURCE_URL}?card_names={encoded_names}"

    try:
        response = requests.get(api_url, timeout=120)
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
        # ✅ Ensure the full card name is stored correctly
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

@app.post("/update-database/")
def update_database(card_names: str = Query(..., description="Comma-separated list of card names")):
    """Fetch updated data for specific cards from the main API and store it in PostgreSQL."""
    return fetch_and_store_data(card_names)

@app.post("/populate-database/")
def populate_database():
    """Fetch all card names from the main API, then request their prices in batches."""
    print("🔍 Fetching all available card names from the API...")

    try:
        response = requests.get(f"{API_SOURCE_URL}?list_all_cards=true", timeout=120)
        
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch card list: {response.status_code}")
            return {"error": f"API request failed: {response.status_code}"}

        all_card_names = response.json().get("all_cards", [])  # ✅ Expecting {"all_cards": ["Black Lotus", "Mox Emerald", ...]}

        if not all_card_names:
            print("❌ No cards found in the main API response.")
            return {"error": "No cards found in API response."}

        print(f"✅ Retrieved {len(all_card_names)} card names. Processing in batches...")

        # ✅ Fetch and store data in batches of 50
        batch_size = 50
        for i in range(0, len(all_card_names), batch_size):
            batch = ",".join(all_card_names[i:i + batch_size])
            fetch_and_store_data(batch)

        print("✅ Database populated successfully!")
        return {"message": "Database populated successfully!"}

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")
        return {"error": str(e)}
