import React, { useState, useEffect } from 'react';

function AirQualityWidget({ city }) {
  const [airQuality, setAirQuality] = useState(null);

  useEffect(() => {
    async function fetchAirQuality() {
      try {
        const response = await fetch(`/api/air_quality?city=${city}`);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        console.log('Fetched air quality data:', data);  // Debugging log
        setAirQuality(data[0]);  // Get the first result from the array
      } catch (error) {
        console.error('Error fetching air quality data:', error);
        setAirQuality({ error: error.message });
      }
    }
    fetchAirQuality();
  }, [city]);

  if (!airQuality) {
    return <div>Loading...</div>;
  }

  return (
    <div className="air-quality-widget">
      <h2>Air Quality in {city}</h2>
      {airQuality.error ? (
        <p>{airQuality.error}</p>
      ) : (
        <div>
          <p>AQI: {airQuality.aqi ? airQuality.aqi : 'No data available'}</p>
          <p>PM2.5: {airQuality.pm25 ? airQuality.pm25 : 'No data available'}</p>
          <p>PM10: {airQuality.pm10 ? airQuality.pm10 : 'No data available'}</p>
        </div>
      )}
    </div>
  );
}

export default AirQualityWidget;
