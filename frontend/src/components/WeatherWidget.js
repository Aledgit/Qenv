import React, { useState, useEffect } from 'react';

function WeatherWidget({ city }) {
  const [weather, setWeather] = useState(null);

  useEffect(() => {
    async function fetchWeather() {
      try {
        const response = await fetch(`/api/weather?city=${city}`);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        console.log('Fetched weather data:', data);  // Debugging log
        setWeather(data);
      } catch (error) {
        console.error('Error fetching weather data:', error);
        setWeather({ error: error.message });
      }
    }
    fetchWeather();
  }, [city]);

  if (!weather) {
    return <div>Loading...</div>;
  }

  return (
    <div className="weather-widget">
      <h2>Weather in {city}</h2>
      {weather.error ? (
        <p>{weather.error}</p>
      ) : (
        <div>
          <p>Temperature: {weather.temperature ? `${weather.temperature} °C` : 'N/A'}</p>
          <p>Humidity: {weather.humidity ? `${weather.humidity}%` : 'N/A'}</p>
        </div>
      )}
    </div>
  );
}

export default WeatherWidget;
