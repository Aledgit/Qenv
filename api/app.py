from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

# Connect to the SQLite database
def get_db_connection():
    conn = sqlite3.connect('data.db')
    conn.row_factory = sqlite3.Row  # To return rows as dictionaries
    return conn

@app.route('/api/weather', methods=['GET'])
def get_latest_weather():
    conn = get_db_connection()
    data = conn.execute('SELECT * FROM historical_data ORDER BY timestamp DESC LIMIT 1').fetchone()
    conn.close()
    return jsonify(dict(data))

@app.route('/api/history', methods=['GET'])
def get_weather_history():
    conn = get_db_connection()
    data = conn.execute('SELECT * FROM historical_data ORDER BY timestamp DESC').fetchall()
    conn.close()
    return jsonify([dict(row) for row in data])

if __name__ == '__main__':
    app.run(debug=True)
