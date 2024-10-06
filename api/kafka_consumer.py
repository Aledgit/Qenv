from kafka import KafkaConsumer
import json
import sqlite3

# Initialize Kafka consumer for both weather and air quality data
consumer = KafkaConsumer(
    'weather-data', 'air-quality-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None  # Handle empty messages
)

# Connect to SQLite database (or another database)
conn = sqlite3.connect('data.db')
cursor = conn.cursor()

# Create a table for weather data if it doesn't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS historical_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT,
                    temperature REAL,
                    humidity REAL,
                    timestamp INTEGER UNIQUE
                )''')

# Create a table for air quality data if it doesn't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS air_quality_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT,
                    aqi INTEGER,
                    pm25 REAL,
                    pm10 REAL,
                    timestamp INTEGER UNIQUE
                )''')

conn.commit()

# Function to save weather data to the database
def save_weather_data_to_db(data):
    city = data['city']
    temperature = data['temperature']
    humidity = data['humidity']
    timestamp = data['timestamp']

    # Insert the weather data into the database if not already present
    cursor.execute('''INSERT OR IGNORE INTO historical_data (city, temperature, humidity, timestamp)
                      VALUES (?, ?, ?, ?)''', (city, temperature, humidity, timestamp))
    conn.commit()

# Function to save air quality data to the database
def save_air_quality_to_db(data):
    city = data['city']['name']
    aqi = data['aqi']
    pm25 = data['iaqi'].get('pm25', {}).get('v', None)
    pm10 = data['iaqi'].get('pm10', {}).get('v', None)
    timestamp = data['time']['v']

    # Insert the air quality data into the database if not already present
    cursor.execute('''INSERT OR IGNORE INTO air_quality_data (city, aqi, pm25, pm10, timestamp)
                      VALUES (?, ?, ?, ?, ?)''', (city, aqi, pm25, pm10, timestamp))
    conn.commit()

# Consume both weather and air quality data
def consume_data():
    for message in consumer:
        try:
            # Check if the message is not empty
            if message.value:
                if message.topic == 'weather-data':
                    # Process weather data
                    weather_data = message.value
                    print(f"Received weather data: {weather_data}")
                    save_weather_data_to_db(weather_data)
                elif message.topic == 'air-quality-data':
                    # Process air quality data
                    air_quality_data = message.value
                    print(f"Received air quality data: {air_quality_data}")
                    save_air_quality_to_db(air_quality_data)
            else:
                print("Received empty message, skipping.")
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
        except KeyError as e:
            print(f"Missing expected field: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    consume_data()
