import React from 'react';
import WeatherWidget from './components/WeatherWidget';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <h1>Real-Time Environmental Data Platform</h1>
        <WeatherWidget city="Santiago" />
      </header>
    </div>
  );
}

export default App;
