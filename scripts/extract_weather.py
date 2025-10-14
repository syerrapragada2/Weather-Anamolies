import requests
import polars as pl
from datetime import datetime

# 🌍 Major cities from East to West
CITIES = {
    "Tokyo_JP": {"lat": 35.6762, "lon": 139.6503, "tz": "Asia/Tokyo"},
    "Hyderabad_IN": {"lat": 17.3850, "lon": 78.4867, "tz": "Asia/Kolkata"},
    "Dubai_AE": {"lat": 25.2048, "lon": 55.2708, "tz": "Asia/Dubai"},
    "Berlin_DE": {"lat": 52.5200, "lon": 13.4050, "tz": "Europe/Berlin"},
    "London_UK": {"lat": 51.5074, "lon": -0.1278, "tz": "Europe/London"},
    "New_York_US": {"lat": 40.7128, "lon": -74.0060, "tz": "America/New_York"},
    "Nashville_US": {"lat": 36.1627, "lon": -86.7816, "tz": "America/Chicago"},
    "Los_Angeles_US": {"lat": 34.0522, "lon": -118.2437, "tz": "America/Los_Angeles"},
    "Sao_Paulo_BR": {"lat": -23.5505, "lon": -46.6333, "tz": "America/Sao_Paulo"},
    "Sydney_AU": {"lat": -33.8688, "lon": 151.2093, "tz": "Australia/Sydney"},
}

def fetch_weather(lat, lon, start_date, end_date, timezone):
    """Fetch historical daily weather data."""
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
        f"&timezone={timezone}"
    )
    print(f"Fetching data for {lat}, {lon} ({timezone})")
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def main():
    start_date = "2022-01-01"
    end_date = "2024-12-31"
    all_data = []

    for city, coords in CITIES.items():
        print(f"🌦️ Fetching {city}...")
        data = fetch_weather(coords["lat"], coords["lon"], start_date, end_date, coords["tz"])
        daily = data["daily"]

        df = pl.DataFrame({
            "city": [city] * len(daily["time"]),
            "date": daily["time"],
            "temp_max": daily["temperature_2m_max"],
            "temp_min": daily["temperature_2m_min"],
            "precipitation": daily["precipitation_sum"],
            "windspeed_max": daily["windspeed_10m_max"],
        })

        all_data.append(df)

    final_df = pl.concat(all_data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/raw/weather_global_history_{timestamp}.csv"
    final_df.write_csv(output_path)

    print(f"✅ Saved global historical weather data to {output_path}")
    print(final_df.group_by("city").count())

if __name__ == "__main__":
    main()
