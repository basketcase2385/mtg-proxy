import requests
import psycopg2
import os
from fastapi import FastAPI

# PostgreSQL Database URL from Render
DATABASE_URL = "postgresql://mtg_database_user:yuy654YGIgOhE1w7jY5Mn2ZZ53K57YNX@dpg-cu9tv73tq21c739akumg-a.oregon-postgres.render.com/mtg_database"

# Define FastAPI app
app = FastAPI()

def get_db_connection():
    """Establish a connection to PostgreSQL."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def create_table():
    """Ensures the database table exists."""
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

create_table()  # Ensure table is created on startup

def fetch_and_store_data():
    """Fetch card prices from the main API via Ngrok and store them in PostgreSQL."""
    API_SOURCE_URL = "https://mtgapp.ngrok.app/fetch_prices"

    try:
        response = requests.get(API_SOURCE_URL)

        # Debugging: Check API response status
        print(f"🔍 API Response Status Code: {response.status_code}")

        # Ensure response is valid JSON
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch data: {response.status_code} - {response.text}")
            return
        
        try:
            data = response.json()  # Convert response to JSON
        except requests.exceptions.JSONDecodeError:
            print("❌ Error: API response is not valid JSON!")
            return
        
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

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")

fetch_and_store_data()

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

@app.post("/update-database/")
def update_database():
    """Fetch updated data from the main API via Ngrok and store it in PostgreSQL."""
    API_SOURCE_URL = "https://mtgapp.ngrok.app/fetch_prices/?card_names=Black%20Lotus,Mox%20Emerald"  # Example cards

    try:
        response = requests.get(API_SOURCE_URL)

        # Debugging: Check API response status
        print(f"🔍 API Response Status Code: {response.status_code}")

        # Ensure response is valid JSON
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch data: {response.status_code} - {response.text}")
            return
        
        try:
            data = response.json()  # Convert response to JSON
        except requests.exceptions.JSONDecodeError:
            print("❌ Error: API response is not valid JSON!")
            return
        
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
        return {"message": "Database updated successfully!"}

    except requests.exceptions.RequestException as e:
        return {"error": f"API request failed: {str(e)}"}

# Ensure we bind to the correct port for Render
PORT = int(os.getenv("PORT", 8000))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("proxy_server:app", host="0.0.0.0", port=PORT, reload=True)
