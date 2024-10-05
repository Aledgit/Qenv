import requests
import os

# Set the base URL for the OpenWeatherMap API
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Function to fetch weather data for a specific city
def get_weather_data(city):
    api_key = os.getenv('OPENWEATHER_API_KEY')  # Fetch the API key from environment variables
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'  # Use metric units for temperature (Celsius)
    }
    response = requests.get(BASE_URL, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        return response.json()  # Return the JSON data
    else:
        return {'error': f"Failed to fetch weather data for {city}"}
