import requests
import psycopg2
import os
from fastapi import FastAPI, Query

# ✅ PostgreSQL Database URL from Render
DATABASE_URL = "postgresql://your_username:your_password@your-db-host:port/your_db_name"

# ✅ Main API URL (Ensure this is correct)
API_SOURCE_URL = "https://mtgapp.ngrok.app/fetch_prices"

app = FastAPI()

def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")

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
    conn.close()

# ✅ Ensure the table is created on startup
create_table()

def fetch_and_store_data(card_names: str):
    """Fetch card prices dynamically from the main API and store them in PostgreSQL."""
    api_url = f"{API_SOURCE_URL}?card_names={card_names.replace(' ', '%20')}"

    try:
        response = requests.get(api_url, timeout=10)  # Timeout after 10 seconds
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

# ✅ Ensure we bind to the correct port for Render
PORT = int(os.getenv("PORT", 8000))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("proxy_server:app", host="0.0.0.0", port=PORT, reload=True)
