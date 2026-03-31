"""
Pre-compute recent morel sightings for all regions.

Fetches current-season morel observations from iNaturalist for
each region, saving as a single JSON file. Users load this file
instead of querying iNaturalist directly.

Designed to run alongside update_weather.py and update_indicators.py
in the scheduled GitHub Actions workflow.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

APP_DATA = Path("app/data")

INATURALIST_URL = "https://api.inaturalist.org/v1/observations"
MORCHELLA_TAXON_ID = 56830

REGIONS = {
    "ne": {
        "name": "New England",
        "center": [42.28, -71.42],
        "radius_km": 200,
    },
    "mi": {
        "name": "Great Lakes",
        "center": [44.3, -85.6],
        "radius_km": 300,
    },
    "mo": {
        "name": "Missouri / Central US",
        "center": [38.5, -92.3],
        "radius_km": 300,
    },
}


def fetch_recent_morels(lat, lon, radius_km):
    """Fetch current-season morel sightings for a region."""
    year = datetime.now().year
    start_date = f"{year}-01-01"

    params = {
        "taxon_id": MORCHELLA_TAXON_ID,
        "lat": lat,
        "lng": lon,
        "radius": radius_km,
        "d1": start_date,
        "per_page": 200,
        "order": "desc",
        "order_by": "observed_on",
    }

    resp = requests.get(INATURALIST_URL, params=params, timeout=30)
    resp.raise_for_status()

    sightings = []
    for obs in resp.json().get("results", []):
        # Get coordinates
        location = obs.get("location")
        if not location:
            geojson = obs.get("geojson")
            if geojson:
                lon_o, lat_o = geojson["coordinates"]
            else:
                continue
        else:
            lat_s, lon_s = location.split(",")
            lat_o, lon_o = float(lat_s), float(lon_s)

        # Taxon info
        taxon = obs.get("taxon") or {}

        # Photo
        photo = None
        photos = obs.get("photos") or []
        if photos and photos[0].get("url"):
            photo = photos[0]["url"].replace("/square.", "/small.")

        sightings.append({
            "id": obs.get("id"),
            "date": obs.get("observed_on"),
            "lat": round(lat_o, 4),
            "lon": round(lon_o, 4),
            "species": taxon.get("name", "Morchella"),
            "common": taxon.get("preferred_common_name"),
            "quality": obs.get("quality_grade"),
            "photo": photo,
            "uri": obs.get("uri"),
        })

    return sightings


def main():
    print(f"Updating recent morel sightings at {datetime.now(timezone.utc).isoformat()}")

    output = {"updated_at": datetime.now(timezone.utc).isoformat(), "regions": {}}

    for region_key, region in REGIONS.items():
        try:
            sightings = fetch_recent_morels(
                region["center"][0],
                region["center"][1],
                region["radius_km"],
            )
            output["regions"][region_key] = {
                "name": region["name"],
                "sightings": sightings,
            }
            print(f"  {region['name']}: {len(sightings)} morel sightings this season")
            time.sleep(1.5)
        except Exception as e:
            print(f"  {region['name']}: ERROR — {e}")
            output["regions"][region_key] = {
                "name": region["name"],
                "sightings": [],
            }

    outfile = APP_DATA / "morels-latest.json"
    with open(outfile, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    total = sum(len(r["sightings"]) for r in output["regions"].values())
    size_kb = outfile.stat().st_size / 1024
    print(f"\nDone! {total} total sightings, {size_kb:.0f} KB")


if __name__ == "__main__":
    main()
