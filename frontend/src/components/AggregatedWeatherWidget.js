import React, { useState, useEffect } from 'react';

function AggregatedWeather() {
  const [weatherData, setWeatherData] = useState([]);

  useEffect(() => {
    async function fetchWeather() {
      const response = await fetch('/api/aggregated_weather');
      const data = await response.json();
      setWeatherData(data);
    }
    fetchWeather();
  }, []);

  if (!weatherData.length) {
    return <div>Loading aggregated weather data...</div>;
  }

  return (
    <div className="weather-aggregates">
      <h2>Aggregated Weather Data</h2>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>City</th>
            <th>Avg Temperature (°C)</th>
          </tr>
        </thead>
        <tbody>
          {weatherData.map((row, index) => (
            <tr key={index}>
              <td>{new Date(row[0]).toLocaleDateString()}</td> {/* Convert the timestamp */}
              <td>{row[1]}</td> {/* City name */}
              <td>{row[2].toFixed(2)}</td> {/* Avg temperature rounded to 2 decimal places */}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default AggregatedWeather;
