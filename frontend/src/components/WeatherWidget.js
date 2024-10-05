import React, { useState, useEffect } from 'react';

function WeatherWidget({ city }) {
  const [weather, setWeather] = useState(null);

  useEffect(() => {
    async function fetchWeather() {
      const response = await fetch(`/api/weather?city=${city}`);
      const data = await response.json();
      setWeather(data);
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
          <p>Temperature: {weather.main.temp} °C</p>
          <p>Humidity: {weather.main.humidity}%</p>
          <p>Wind Speed: {weather.wind.speed} m/s</p>
        </div>
      )}
    </div>
  );
}

export default WeatherWidget;
