from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

# Connect to the SQLite database
def get_db_connection():
    conn = sqlite3.connect('data.db')
    conn.row_factory = sqlite3.Row  # To return rows as dictionaries
    return conn

@app.route('/api/weather', methods=['GET'])
def get_latest_weather():
    city = request.args.get('city')
    conn = get_db_connection()
    data = conn.execute("SELECT * FROM historical_data WHERE city = ? ORDER BY timestamp DESC LIMIT 1", (city,)).fetchone()
    conn.close()
    return jsonify(dict(data))

@app.route('/api/history', methods=['GET'])
def get_weather_history():
    city = request.args.get('city')
    conn = get_db_connection()
    data = conn.execute("SELECT * FROM historical_data WHERE city = ? ORDER BY timestamp DESC", (city,)).fetchall()
    conn.close()
    return jsonify([dict(row) for row in data])

city_name_mapping = {
    'Santiago': "Parque O'Higgins, Chile",
    # Add more mappings if necessary
}

@app.route('/api/air_quality', methods=['GET'])
def get_air_quality():
    city = request.args.get('city')

    # Map the city name to what's stored in the database
    db_city_name = city_name_mapping.get(city, city)  # Use the mapped name or fallback to the original

    # Connect to your SQLite database
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()

    # Execute the SQL query to get the air quality data
    cursor.execute("SELECT * FROM air_quality_data WHERE city = ?", (db_city_name,))
    data = cursor.fetchall()

    # Check if the query returns data
    if not data:
        return jsonify({'error': f'No air quality data found for {city}.'})

    # Assuming the data is a list of rows, convert it to dictionaries
    column_names = [description[0] for description in cursor.description]
    result = [dict(zip(column_names, row)) for row in data]

    return jsonify(result)

# Connect to the database and fetch aggregated weather data
@app.route('/api/aggregated_weather', methods=['GET'])
def get_aggregated_weather():
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM daily_avg_weather ORDER BY date DESC LIMIT 10")
    rows = cursor.fetchall()
    conn.close()
    return jsonify(rows)

# Connect to the database and fetch aggregated air quality data
@app.route('/api/aggregated_air_quality', methods=['GET'])
def get_aggregated_air_quality():
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM daily_avg_air_quality ORDER BY date DESC LIMIT 10")
    rows = cursor.fetchall()
    conn.close()
    return jsonify(rows)

if __name__ == '__main__':
    app.run(debug=True)
