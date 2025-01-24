import requests
import psycopg2
import os
from fastapi import FastAPI

DATABASE_URL = "postgresql://mtg_database_user:yuy654YGIgOhE1w7jY5Mn2ZZ53K57YNX@dpg-cu9tv73tq21c739akumg-a.oregon-postgres.render.com/mtg_database"

app = FastAPI()

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def fetch_and_store_data():
    """Fetch full dataset and store it in PostgreSQL"""
    API_SOURCE_URL = "https://your-existing-api.ngrok.io/full-dataset"

    try:
        response = requests.get(API_SOURCE_URL)

        # 📌 Debugging: Check the response
        print(f"🔍 API Response Status Code: {response.status_code}")

        # 📌 Ensure the response is valid JSON
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

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cards (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE,
                set_name TEXT,
                price REAL
            )
        """)

        for card in data:
            cursor.execute("""
                INSERT INTO cards (name, set_name, price)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET 
                set_name = EXCLUDED.set_name,
                price = EXCLUDED.price
            """, (card["name"], card["set"], card["price"]))

        conn.commit()
        conn.close()
        print("✅ Data successfully stored in PostgreSQL!")

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {str(e)}")

fetch_and_store_data()
