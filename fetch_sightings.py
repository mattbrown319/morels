"""
Fetch morel mushroom observations from iNaturalist API.

Pulls all Morchella observations with coordinates, saving to CSV.
iNaturalist API docs: https://api.inaturalist.org/v1/docs/
"""

import csv
import sys
import time
from pathlib import Path

import requests

API_URL = "https://api.inaturalist.org/v1/observations"
TAXON_NAME = "Morchella"
PER_PAGE = 200  # max allowed by iNaturalist
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "morel_sightings.csv"

# Fields we want to extract from each observation
CSV_FIELDS = [
    "id",
    "observed_on",
    "latitude",
    "longitude",
    "place_guess",
    "quality_grade",
    "species_guess",
    "taxon_name",
    "taxon_common_name",
    "taxon_rank",
    "taxon_id",
    "user_login",
    "num_identification_agreements",
    "num_identification_disagreements",
    "photo_url",
    "created_at",
    "uri",
]


def extract_record(obs: dict) -> dict:
    """Extract relevant fields from a single iNaturalist observation."""
    taxon = obs.get("taxon") or {}
    location = obs.get("location")
    lat, lon = (None, None)
    if location:
        parts = location.split(",")
        if len(parts) == 2:
            lat, lon = parts[0].strip(), parts[1].strip()

    photos = obs.get("photos") or []
    photo_url = None
    if photos:
        # Replace "square" with "medium" for a better resolution image URL
        photo_url = photos[0].get("url", "").replace("/square.", "/medium.")

    return {
        "id": obs.get("id"),
        "observed_on": obs.get("observed_on"),
        "latitude": lat,
        "longitude": lon,
        "place_guess": obs.get("place_guess"),
        "quality_grade": obs.get("quality_grade"),
        "species_guess": obs.get("species_guess"),
        "taxon_name": taxon.get("name"),
        "taxon_common_name": taxon.get("preferred_common_name"),
        "taxon_rank": taxon.get("rank"),
        "taxon_id": taxon.get("id"),
        "user_login": (obs.get("user") or {}).get("login"),
        "num_identification_agreements": obs.get("num_identification_agreements"),
        "num_identification_disagreements": obs.get("num_identification_disagreements"),
        "photo_url": photo_url,
        "created_at": obs.get("created_at"),
        "uri": obs.get("uri"),
    }


def fetch_all_observations(quality_grade=None):
    """
    Fetch all Morchella observations from iNaturalist.

    Uses id_above pagination (recommended by iNaturalist) to iterate through
    all results efficiently.
    """
    params = {
        "taxon_name": TAXON_NAME,
        "per_page": PER_PAGE,
        "order": "asc",
        "order_by": "id",
        "geo": "true",  # only observations with coordinates
    }
    if quality_grade:
        params["quality_grade"] = quality_grade

    all_records = []
    id_above = 0
    page_num = 0

    # First, get total count
    count_params = {**params, "per_page": 0}
    resp = requests.get(API_URL, params=count_params, timeout=30)
    resp.raise_for_status()
    total = resp.json()["total_results"]
    print(f"Total observations to fetch: {total:,}")

    while True:
        page_num += 1
        params["id_above"] = id_above

        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"\nRequest error on page {page_num}: {e}")
            print("Waiting 60s before retry...")
            time.sleep(60)
            continue

        data = resp.json()
        results = data.get("results", [])

        if not results:
            break

        for obs in results:
            record = extract_record(obs)
            all_records.append(record)

        id_above = results[-1]["id"]
        fetched = len(all_records)
        pct = (fetched / total * 100) if total else 0
        print(f"\r  Fetched {fetched:,} / {total:,} ({pct:.1f}%)", end="", flush=True)

        # Respect rate limits — iNaturalist asks for ~1 req/sec
        time.sleep(1.0)

    print()  # newline after progress
    return all_records


def save_to_csv(records, output_path):
    """Save records to CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(records)
    print(f"Saved {len(records):,} records to {output_path}")


def main():
    print("Fetching morel (Morchella) observations from iNaturalist...")
    print("This will take a while due to rate limiting (~1 request/sec).\n")

    records = fetch_all_observations()

    if not records:
        print("No records fetched!")
        sys.exit(1)

    save_to_csv(records, OUTPUT_FILE)

    # Print quick summary
    quality_counts = {}
    species_counts = {}
    year_counts = {}
    for r in records:
        qg = r["quality_grade"] or "unknown"
        quality_counts[qg] = quality_counts.get(qg, 0) + 1

        sp = r["taxon_name"] or "unknown"
        species_counts[sp] = species_counts.get(sp, 0) + 1

        if r["observed_on"]:
            year = r["observed_on"][:4]
            year_counts[year] = year_counts.get(year, 0) + 1

    print(f"\n--- Summary ---")
    print(f"Total records: {len(records):,}")

    print(f"\nBy quality grade:")
    for qg, count in sorted(quality_counts.items(), key=lambda x: -x[1]):
        print(f"  {qg}: {count:,}")

    print(f"\nTop 15 species:")
    for sp, count in sorted(species_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {sp}: {count:,}")

    print(f"\nBy year (last 10):")
    for year, count in sorted(year_counts.items())[-10:]:
        print(f"  {year}: {count:,}")


if __name__ == "__main__":
    main()
