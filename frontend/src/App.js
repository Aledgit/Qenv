import React from 'react';
import WeatherWidget from './components/WeatherWidget';
import AirQualityWidget from './components/AirQualityWidget';
import AggregatedWeatherWidget from './components/AggregatedWeatherWidget';


function App() {
  return (
    <div className="App">
      <header className="App-header">
        <h1>Real-Time Environmental Data Platform</h1>
        <WeatherWidget city="Santiago" />
        <AirQualityWidget city="Santiago" />
        <AggregatedWeatherWidget />
      </header>
    </div>
  );
}

export default App;
