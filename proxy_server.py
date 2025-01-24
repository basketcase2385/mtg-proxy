import requests
from fastapi import FastAPI
import sqlite3

# Set up your existing API URL (the one from your current ngrok)
API_SOURCE_URL = "https://your-existing-api.ngrok.io/full-dataset"

app = FastAPI()

# Function to fetch and store data
def fetch_and_store_data():
    response = requests.get(API_SOURCE_URL)
    data = response.json()  # Assuming the response is JSON
    conn = sqlite3.connect("mtg_cards.db")
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cards (
        name TEXT,
        set_name TEXT,
        price REAL
    )
    """)
    
    cursor.executemany("INSERT INTO cards (name, set_name, price) VALUES (?, ?, ?)", 
                        [(card["name"], card["set"], card["price"]) for card in data])
    
    conn.commit()
    conn.close()

# Fetch data on startup
fetch_and_store_data()

@app.get("/card/{card_name}")
def get_card(card_name: str):
    conn = sqlite3.connect("mtg_cards.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM cards WHERE name = ?", (card_name,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {"name": result[0], "set": result[1], "price": result[2]}
    return {"error": "Card not found"}
