"""
Fetch historical daily weather for Arlo Williamsburg (2024-2025) from Open-Meteo.

Endpoint : https://archive-api.open-meteo.com/v1/archive
Location : Williamsburg, Brooklyn — lat 40.7178, lon -73.9575
Units    : Fahrenheit, mph, inches
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import requests
from app.config import PROCESSED_DIR

LATITUDE   = 40.7178
LONGITUDE  = -73.9575
TIMEZONE   = "America/New_York"
DATE_START = "2024-01-01"
DATE_END   = "2025-12-31"

DAILY_VARS = ",".join([
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "snowfall_sum",
    "windspeed_10m_max",
    "weathercode",
])

RENAME = {
    "temperature_2m_max":  "temp_max_f",
    "temperature_2m_min":  "temp_min_f",
    "temperature_2m_mean": "temp_mean_f",
    "precipitation_sum":   "precipitation_in",
    "snowfall_sum":        "snowfall_in",
    "windspeed_10m_max":   "windspeed_max_mph",
    "weathercode":         "weathercode",
}

# WMO code → human-readable label (coarse groupings)
WMO_LABELS = {
    0:  "Clear sky",
    1:  "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Drizzle", 55: "Heavy drizzle",
    61: "Slight rain", 63: "Rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Snow", 75: "Heavy snow",
    77: "Snow grains",
    80: "Slight showers", 81: "Showers", 82: "Violent showers",
    85: "Snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm w/ hail", 99: "Thunderstorm w/ heavy hail",
}


def fetch_weather() -> pd.DataFrame:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":           LATITUDE,
        "longitude":          LONGITUDE,
        "start_date":         DATE_START,
        "end_date":           DATE_END,
        "daily":              DAILY_VARS,
        "timezone":           TIMEZONE,
        "temperature_unit":   "fahrenheit",
        "windspeed_unit":     "mph",
        "precipitation_unit": "inch",
    }
    print(f"  Requesting Open-Meteo archive ({DATE_START} to {DATE_END})...")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    daily_data = data["daily"]
    df = pd.DataFrame(daily_data)
    df = df.rename(columns={"time": "business_date"})
    df["business_date"] = pd.to_datetime(df["business_date"])
    df = df.rename(columns=RENAME)

    # Weather description from WMO code
    df["weather_description"] = df["weathercode"].map(WMO_LABELS).fillna("Unknown")

    # Derived: precipitation flag, snow flag
    df["had_precipitation"] = df["precipitation_in"] > 0
    df["had_snow"]          = df["snowfall_in"] > 0

    return df


def main():
    print("Fetching historical weather for Williamsburg, Brooklyn (2024-2025)...")

    df = fetch_weather()

    col_order = [
        "business_date",
        "temp_mean_f", "temp_max_f", "temp_min_f",
        "precipitation_in", "had_precipitation",
        "snowfall_in", "had_snow",
        "windspeed_max_mph",
        "weathercode", "weather_description",
    ]
    df = df[[c for c in col_order if c in df.columns]]

    out_path = PROCESSED_DIR / "weather_daily.csv"
    df.to_csv(out_path, index=False)

    print(f"\nweather_daily.csv")
    print(f"  Rows    : {len(df):,}")
    print(f"  Columns : {len(df.columns)} — {list(df.columns)}")
    print()
    print("  Summary stats:")
    print(f"    Temp range     : {df['temp_min_f'].min():.1f}°F – {df['temp_max_f'].max():.1f}°F")
    print(f"    Avg temp       : {df['temp_mean_f'].mean():.1f}°F")
    print(f"    Rain days      : {df['had_precipitation'].sum()} ({df['had_precipitation'].mean()*100:.0f}%)")
    print(f"    Snow days      : {df['had_snow'].sum()} ({df['had_snow'].mean()*100:.0f}%)")
    print(f"    Max wind speed : {df['windspeed_max_mph'].max():.1f} mph")
    print(f"\n  Saved: {out_path}")


if __name__ == "__main__":
    main()
