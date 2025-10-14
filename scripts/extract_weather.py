import requests
import polars as pl
from datetime import datetime

# List of cities with coordinates
CITIES = {
    "New_York": {"lat": 40.71, "lon": -74.01},
    "Los_Angeles": {"lat": 34.05, "lon": -118.24},
    "Chicago": {"lat": 41.88, "lon": -87.63},
}

def fetch_weather(lat, lon):
    """Fetch daily weather data from Open-Meteo API."""
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        "&timezone=America%2FNew_York"
    )
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def main():
    all_data = []
    for city, coords in CITIES.items():
        data = fetch_weather(coords["lat"], coords["lon"])
        daily = data["daily"]
        df = pl.DataFrame({
            "city": [city] * len(daily["time"]),
            "date": daily["time"],
            "temp_max": daily["temperature_2m_max"],
            "temp_min": daily["temperature_2m_min"],
            "precipitation": daily["precipitation_sum"],
        })
        all_data.append(df)

    # Combine all city data
    final_df = pl.concat(all_data)

    # Save to CSV in data/raw/
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/raw/weather_{timestamp}.csv"
    final_df.write_csv(output_path)

    print(f"✅ Weather data saved to {output_path}")

if __name__ == "__main__":
    main()
