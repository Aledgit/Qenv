import requests
import json
import time
import os
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to fetch weather data from OpenWeatherMap API
def fetch_weather_data(city):
    api_key = os.getenv('OPENWEATHER_API_KEY')
    print(api_key)
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad HTTP responses
        data = response.json()

        # Extract necessary data from API response
        weather_data = {
            "city": data["name"],
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "timestamp": data["dt"]
        }
        return weather_data
    except requests.RequestException as e:
        print(f"Error fetching data for {city}: {e}")
        return None

# Function to send data to Kafka topic
def send_data_to_kafka():
    cities = ["Santiago", "New York", "London"]
    
    for city in cities:
        weather_data = fetch_weather_data(city)
        if weather_data:
            # Send the weather data to Kafka topic
            producer.send('weather-data', weather_data)
            print(f"Sent data to Kafka: {weather_data}")

if __name__ == "__main__":
    try:
        while True:
            send_data_to_kafka()
            time.sleep(600)  # Fetch and send data every 10 minutes
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()
