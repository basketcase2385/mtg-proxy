import os
import psycopg2
import requests
from fastapi import FastAPI, Query

# ✅ Use the correct PostgreSQL URL from Render
DATABASE_URL = "postgresql://mtg_database_user:yuy654YGIgOhE1w7jY5Mn2ZZ53K57YNX@dpg-cu9tv73tq21c739akumg-a.oregon-postgres.render.com/mtg_database"

app = FastAPI()

def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Database Connection Error: {e}")
        raise

def create_table():
    """Ensure the database table exists before storing data."""
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

# ✅ Ensure the table is created on startup
create_table()

@app.get("/")
def home():
    """Health check endpoint."""
    return {"message": "MTG Proxy API is running with PostgreSQL!"}

@app.get("/card/{card_name}")
def get_card(card_name: str):
    """Fetch card details from PostgreSQL."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT name, set_name, price FROM cards WHERE LOWER(name) = LOWER(%s)", (card_name,))
    result = cursor.fetchone()
    conn.close()

    if result:
        return {"name": result[0], "set": result[1], "price": result[2]}
    
    return {"error": "Card not found"}

# ✅ Define the main API URL to fetch card prices
API_SOURCE_URL = "https://mtgapp.ngrok.app/fetch_prices/"

def fetch_and_store_data(card_names: str):
    """Fetch card prices dynamically from the main API and store them in PostgreSQL."""
    api_url = f"{API_SOURCE_URL}?card_names={card_names.replace(' ', '%20')}"

    try:
        response = requests.get(api_url, timeout=10)
        print(f"🔍 API Response Status Code: {response.status_code}")

        if response.status_code != 200:
            print(f"⚠️ Failed to fetch data: {response.status_code} - {response.text}")
            return {"error": f"API request failed: {response.status_code}"}

        data = response.json()
        conn = get_db_connection()
        cursor = conn.cursor()

        for name, details in data.items():
            cursor.execute("""
                INSERT INTO cards (name, set_name, price)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET 
                set_name = EXCLUDED.set_name,
                price = EXCLUDED.price
            """, (name, details["set"], details["price"]))

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Data successfully stored in PostgreSQL!")
        return {"message": "Database updated successfully!"}

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")
        return {"error": str(e)}

@app.post("/update-database/")
def update_database(card_names: str = Query(..., description="Comma-separated list of card names")):
    """Fetch updated data for specific cards from the main API and store it in PostgreSQL."""
    return fetch_and_store_data(card_names)

@app.post("/populate-database/")
def populate_database():
    """Fetch all card data from the main API and populate the PostgreSQL database."""

    api_url = f"{API_SOURCE_URL}?card_names=all"  # Assuming API supports fetching all cards
    try:
        response = requests.get(api_url, timeout=30)  # Increase timeout for large data
        print(f"🔍 API Response Status Code: {response.status_code}")

        if response.status_code != 200:
            print(f"⚠️ Failed to fetch data: {response.status_code} - {response.text}")
            return {"error": f"API request failed: {response.status_code}"}

        data = response.json()
        conn = get_db_connection()
        cursor = conn.cursor()

        for name, details in data.items():
            cursor.execute("""
                INSERT INTO cards (name, set_name, price)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET 
                set_name = EXCLUDED.set_name,
                price = EXCLUDED.price
            """, (name, details["set"], details["price"]))

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Database populated successfully!")
        return {"message": "Database populated successfully!"}

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")
        return {"error": str(e)}
