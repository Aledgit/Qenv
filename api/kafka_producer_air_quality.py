from kafka import KafkaProducer
import json
import os
import requests
import time

WAQI_API_BASE_URL = "https://api.waqi.info/feed"
AIR_QUALITY_TOPIC = "air-quality-data"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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

# Function to send air quality data to Kafka
def send_air_quality_to_kafka(city):
    data = get_air_quality(city)
    if 'error' not in data:
        producer.send(AIR_QUALITY_TOPIC, data)
        print(f"Sent air quality data to Kafka for {city}: {data}")
    else:
        print(f"Error fetching air quality data for {city}: {data['error']}")

if __name__ == "__main__":
    city = 'Santiago'  # Example city
    try:
        # Fetch and send air quality data every 10 minutes
        while True:
            send_air_quality_to_kafka(city)
            time.sleep(600)  # 600 seconds = 10 minutes
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()
