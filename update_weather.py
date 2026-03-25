"""
Pre-compute weather data for all regions.

Fetches soil temperature and precipitation from Open-Meteo for every
grid cell across all regions, saving the result as a single JSON file
that the web app loads instead of making client-side API calls.

Designed to run as a scheduled GitHub Actions job (2x/day).
"""

import json
import time
import sys
from pathlib import Path
from datetime import datetime, timezone

import requests

APP_DATA = Path("app/data")

REGIONS = ["ne", "mi", "mo"]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(lat, lon):
    """Fetch soil temp and precipitation for a single grid cell."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "soil_temperature_6cm",
        "daily": "precipitation_sum",
        "past_days": 14,
        "forecast_days": 7,
        "timezone": "auto",
    }

    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Current soil temp (latest non-null)
    soil_temps = [v for v in (data.get("hourly", {}).get("soil_temperature_6cm") or []) if v is not None]
    soil_temp = soil_temps[-1] if soil_temps else None

    # 7-day average soil temp (last 168 hours)
    recent = soil_temps[-168:] if len(soil_temps) >= 168 else soil_temps
    soil_avg = round(sum(recent) / len(recent), 2) if recent else None

    # 7-day forecast average
    forecast = soil_temps[-168:] if soil_temps else []
    forecast_avg = round(sum(forecast) / len(forecast), 2) if forecast else None

    # 14-day precipitation
    daily_precip = (data.get("daily", {}).get("precipitation_sum") or [])[:14]
    precip_14d = round(sum(p for p in daily_precip if p is not None), 1)

    # 7-day forecast precip
    forecast_precip = (data.get("daily", {}).get("precipitation_sum") or [])[14:]
    precip_7d_forecast = round(sum(p for p in forecast_precip if p is not None), 1)

    # Daily soil temps for date estimation
    hourly_times = data.get("hourly", {}).get("time") or []
    hourly_soil = data.get("hourly", {}).get("soil_temperature_6cm") or []
    daily_avgs = {}
    for t, v in zip(hourly_times, hourly_soil):
        if v is None:
            continue
        day = t.split("T")[0]
        daily_avgs.setdefault(day, []).append(v)
    daily_soil = [
        {"d": day, "t": round(sum(temps) / len(temps), 1)}
        for day, temps in sorted(daily_avgs.items())
    ]

    return {
        "st": soil_temp,           # soil temp current
        "sa": soil_avg,            # soil temp 7-day avg
        "sf": forecast_avg,        # soil temp forecast avg
        "p14": precip_14d,         # precip 14-day total
        "p7f": precip_7d_forecast, # precip 7-day forecast
        "ds": daily_soil,          # daily soil temps for date estimation
    }


def update_all_regions():
    """Fetch weather for all grid cells across all regions."""
    all_weather = {}
    total_cells = 0
    errors = 0

    for region in REGIONS:
        grid_file = APP_DATA / f"grid-{region}.json"
        if not grid_file.exists():
            print(f"  Skipping {region} — grid file not found")
            continue

        with open(grid_file) as f:
            cells = json.load(f)

        print(f"  {region}: {len(cells)} cells")
        total_cells += len(cells)

        for i, cell in enumerate(cells):
            key = f"{cell['lat']},{cell['lon']}"
            if key in all_weather:
                continue  # skip duplicates across regions

            try:
                weather = fetch_weather(cell["lat"], cell["lon"])
                all_weather[key] = weather
            except Exception as e:
                errors += 1
                if errors % 50 == 0:
                    print(f"    {errors} errors so far, last: {e}")
                if errors >= 200:
                    print(f"    Too many errors, stopping.")
                    break
                time.sleep(2)
                continue

            if (i + 1) % 50 == 0:
                print(f"    {i + 1}/{len(cells)}...")

            # Rate limit: ~3 req/s to stay well within 600/min
            time.sleep(0.35)

    return all_weather, total_cells, errors


def main():
    print(f"Updating weather data at {datetime.now(timezone.utc).isoformat()}")
    print(f"Regions: {', '.join(REGIONS)}")

    weather, total, errors = update_all_regions()

    # Add metadata
    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "cell_count": len(weather),
        "cells": weather,
    }

    outfile = APP_DATA / "weather-latest.json"
    with open(outfile, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = outfile.stat().st_size / 1024
    print(f"\nDone!")
    print(f"  Cells fetched: {len(weather)}")
    print(f"  Errors: {errors}")
    print(f"  Output: {outfile} ({size_kb:.0f} KB)")


if __name__ == "__main__":
    main()
