from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# Base URL for your MTG API
API_BASE_URL = "https://mtgapp.ngrok.app"

@app.route('/fetch_prices', methods=['GET'])
def fetch_prices():
    card_names = request.args.get('card_names')
    if not card_names:
        return jsonify({"error": "card_names parameter is required"}), 400

    # Forward request to the actual API
    response = requests.get(f"{API_BASE_URL}/fetch_prices", params={"card_names": card_names})
    
    if response.status_code != 200:
        return jsonify({"error": "Failed to fetch data from MTG API"}), response.status_code

    return response.json()

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "Proxy server is running"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
#new useless line