from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from apscheduler.schedulers.background import BackgroundScheduler
from weather_service import get_weather_data
from air_quality_service import get_air_quality
from dotenv import load_dotenv
from datetime import datetime, timezone
import os
import atexit

# Initialize Flask app
app = Flask(__name__)

# Load environment variables
load_dotenv()

# Configure SQLite database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Disable SQLAlchemy event system

# Initialize the database
db = SQLAlchemy(app)

# Define model for historical data
class HistoricalData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    city = db.Column(db.String(50), nullable=False)
    temperature = db.Column(db.Float)
    humidity = db.Column(db.Float)
    aqi = db.Column(db.Integer)  # Air Quality Index
    timestamp = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    def __repr__(self):
        return f'<HistoricalData {self.city} {self.timestamp}>'

# Create the database tables if they don't exist
with app.app_context():
    db.create_all()

def save_historical_data():
    with app.app_context():  # Ensure the task runs within Flask app context
        city = "Santiago"
        print(f"Scheduler triggered to fetch data for {city} at {datetime.now(timezone.utc)}")

        # Fetch weather data
        weather_data = get_weather_data(city)
        if weather_data and "main" in weather_data:
            temperature = weather_data["main"]["temp"]
            humidity = weather_data["main"]["humidity"]
        else:
            temperature = None
            humidity = None

        # Fetch air quality data
        air_quality_data = get_air_quality(city)
        if air_quality_data and "aqi" in air_quality_data:
            aqi = air_quality_data["aqi"]
        else:
            aqi = None

        # Save the data in the database
        historical_data = HistoricalData(
            city=city,
            temperature=temperature,
            humidity=humidity,
            aqi=aqi
        )
        db.session.add(historical_data)
        db.session.commit()

        print(f"Saved historical data for {city} at {datetime.now(timezone.utc)}")

# Set up the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(save_historical_data, 'interval', hours=1)  # Runs every hour
scheduler.start()

# Run it once inmediately at startup, for test purposes
save_historical_data()

# Ensure scheduler shuts down gracefully when app stops
atexit.register(lambda: scheduler.shutdown())

# Route to view historical data
@app.route('/api/history', methods=['GET'])
def history():
    city = request.args.get('city', default='Santiago')

    # Get the start and end dates from the query parameters
    start_date_str = request.args.get('start', default='2024-01-01')
    end_date_str = request.args.get('end', default=datetime.now(timezone.utc).strftime('%Y-%m-%d'))

    # Convert the start and end date strings to timezone-aware datetime objects
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.'}), 400

    # Query the database for historical data within the date range
    historical_data = HistoricalData.query.filter(
        HistoricalData.city == city,
        HistoricalData.timestamp >= start_date,
        HistoricalData.timestamp <= end_date
    ).all()

    # Log the queried data to debug
    print(f"Queried data for {city}: {historical_data}")

    # Convert to list of dictionaries for JSON response
    history_list = [
        {
            "city": data.city,
            "temperature": data.temperature,
            "humidity": data.humidity,
            "aqi": data.aqi,
            "timestamp": data.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        }
        for data in historical_data
    ]

    return jsonify(history_list)

# Default home route
@app.route('/')
def home():
    return "Welcome to the Real-Time Environmental Data Monitoring Platform!"

# Weather endpoint
@app.route('/api/weather', methods=['GET'])
def weather():
    city = request.args.get('city', default='Santiago')
    weather_data = get_weather_data(city)
    return jsonify(weather_data)

# Air quality endpoint
@app.route('/api/air-quality', methods=['GET'])
def air_quality():
    city = request.args.get('city', default='Santiago')
    air_quality_data = get_air_quality(city)
    return jsonify(air_quality_data)

if __name__ == "__main__":
    # Start Flask app
    app.run(host='0.0.0.0', port=5000, debug=True)

