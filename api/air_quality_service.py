import os
import requests

WAQI_API_BASE_URL = "https://api.waqi.info/feed"

def get_air_quality(city):
    api_key = os.getenv('WAQI_API_KEY')

    if not api_key:
        return {'error': 'API key for Air Quality service is missing.'}
    
    # Request the air quality data from the API

    url = f"{WAQI_API_BASE_URL}/{city}/?token={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        # Handle invalid city input
        if "data" in data and data["status"] == "ok":
            return data["data"]
        
        else:
            return {'error': f'Failed to fetch air quality data for {city}.'}
        
    else:
        return {'error': f'HTTP error {response.status_code} when fetching air quality data.'}