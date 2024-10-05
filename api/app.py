from flask import Flask, request, jsonify
from weather_service import get_weather_data
from dotenv import load_dotenv
import os

app = Flask(__name__)

# Load environment variables from .env
load_dotenv()

print(f"API Key: {os.getenv('OPENWEATHER_API_KEY')}")

@app.route('/')
def home():
    return "Welcome to the Real-Time Environmental Data Monitoring Platform!"

# Define the /api/weather endpoint
@app.route('/api/weather', methods=['GET'])
def weather():
    city = request.args.get('city', default='Santiago')  # Default to 'Santiago' if no city is provided
    weather_data = get_weather_data(city)  # Call the weather service
    return jsonify(weather_data)  # Return the data as a JSON response

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
