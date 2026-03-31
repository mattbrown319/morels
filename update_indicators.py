"""
Pre-compute indicator species sightings for all regions.

Fetches recent observations from iNaturalist for each region's
indicator species, saving as a single JSON file that the app
loads instead of making client-side API calls.

Designed to run alongside update_weather.py in the scheduled
GitHub Actions workflow.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

APP_DATA = Path("app/data")

INATURALIST_URL = "https://api.inaturalist.org/v1/observations"

# Region-specific indicator species, derived from our co-occurrence analysis
# of 49K morel sightings against iNaturalist data.
REGION_INDICATORS = {
    "ne": {
        "name": "New England / Eastern",
        "center": [42.28, -71.42],
        "radius_km": 200,
        "taxa": [
            {"id": 60714, "name": "Alliaria petiolata", "common": "Garlic Mustard",
             "why": "#1 co-occurring species. Thrives in same moist deciduous habitat.", "icon": "leaf"},
            {"id": 47585, "name": "Sanguinaria canadensis", "common": "Bloodroot",
             "why": "Early spring bloomer in rich deciduous woods.", "icon": "flower"},
            {"id": 56077, "name": "Erythronium americanum", "common": "Yellow Trout Lily",
             "why": "Unique eastern indicator. Blooms during morel season.", "icon": "flower"},
            {"id": 49968, "name": "Claytonia virginica", "common": "Virginia Springbeauty",
             "why": "Carpets forest floors right as morels appear.", "icon": "flower"},
            {"id": 56955, "name": "Podophyllum peltatum", "common": "Mayapple",
             "why": "Classic indicator — first leaves = first morels (2 days apart).", "icon": "leaf"},
            {"id": 48226, "name": "Symplocarpus foetidus", "common": "Eastern Skunk Cabbage",
             "why": "Earliest spring emergence. When it's up, soil is thawing.", "icon": "leaf"},
            {"id": 50310, "name": "Dicentra cucullaria", "common": "Dutchman's Breeches",
             "why": "Blooms just before peak morel time in rich woods.", "icon": "flower"},
            {"id": 47599, "name": "Trillium", "common": "Trillium",
             "why": "Traditional indicator — 'when trillium blooms, look for morels.'", "icon": "flower"},
            {"id": 940028, "name": "Cerioporus squamosus", "common": "Dryad's Saddle",
             "why": "Top co-occurring fungus. Found on same dead hardwoods.", "icon": "mushroom"},
        ],
    },
    "mi": {
        "name": "Great Lakes",
        "center": [44.3, -85.6],
        "radius_km": 250,
        "taxa": [
            {"id": 60714, "name": "Alliaria petiolata", "common": "Garlic Mustard",
             "why": "#1 co-occurring species across the eastern deciduous belt.", "icon": "leaf"},
            {"id": 49968, "name": "Claytonia virginica", "common": "Virginia Springbeauty",
             "why": "Blankets forest floors at morel time.", "icon": "flower"},
            {"id": 47585, "name": "Sanguinaria canadensis", "common": "Bloodroot",
             "why": "Early spring ephemeral in the same rich woods.", "icon": "flower"},
            {"id": 56077, "name": "Erythronium americanum", "common": "Yellow Trout Lily",
             "why": "Blooms in deciduous forests during morel season.", "icon": "flower"},
            {"id": 56955, "name": "Podophyllum peltatum", "common": "Mayapple",
             "why": "Leaves unfurling = morels are out.", "icon": "leaf"},
            {"id": 47599, "name": "Trillium", "common": "Trillium",
             "why": "Traditional indicator — 'when trillium blooms, look for morels.'", "icon": "flower"},
            {"id": 50310, "name": "Dicentra cucullaria", "common": "Dutchman's Breeches",
             "why": "Rich-woods ephemeral, blooms in the morel window.", "icon": "flower"},
            {"id": 940028, "name": "Cerioporus squamosus", "common": "Dryad's Saddle",
             "why": "Top fungus co-occurrence — same dead hardwood habitat.", "icon": "mushroom"},
        ],
    },
    "mo": {
        "name": "Missouri / Central US",
        "center": [38.5, -92.3],
        "radius_km": 250,
        "taxa": [
            {"id": 60714, "name": "Alliaria petiolata", "common": "Garlic Mustard",
             "why": "Top co-occurring plant in Central US morel habitats.", "icon": "leaf"},
            {"id": 49968, "name": "Claytonia virginica", "common": "Virginia Springbeauty",
             "why": "Strong #2 indicator in the Central/Plains region.", "icon": "flower"},
            {"id": 51502, "name": "Cardamine concatenata", "common": "Cut-leaved Toothwort",
             "why": "Spring ephemeral prominent in MO morel habitat.", "icon": "flower"},
            {"id": 50310, "name": "Dicentra cucullaria", "common": "Dutchman's Breeches",
             "why": "More prominent in Central US than other regions.", "icon": "flower"},
            {"id": 47585, "name": "Sanguinaria canadensis", "common": "Bloodroot",
             "why": "Rich-woods ephemeral, same habitat as morels.", "icon": "flower"},
            {"id": 76350, "name": "Phlox divaricata", "common": "Blue Phlox",
             "why": "Distinctive Central US indicator — prominent in MO data.", "icon": "flower"},
            {"id": 56955, "name": "Podophyllum peltatum", "common": "Mayapple",
             "why": "First leaves appear same week as first morels.", "icon": "leaf"},
            {"id": 972414, "name": "Urnula craterium", "common": "Devil's Urn",
             "why": "Distinctive spring fungus — strong Central US signal.", "icon": "mushroom"},
        ],
    },
}


def fetch_recent_sightings(taxon_id, lat, lon, radius_km, days_back=45):
    """Fetch recent observations of an indicator species."""
    year = datetime.now().year
    start_date = f"{year}-01-01"

    params = {
        "taxon_id": taxon_id,
        "lat": lat,
        "lng": lon,
        "radius": radius_km,
        "d1": start_date,
        "quality_grade": "research,needs_id",
        "per_page": 50,
        "order": "desc",
        "order_by": "observed_on",
        "fields": "id,observed_on,location,taxon,photos,uri",
    }

    resp = requests.get(INATURALIST_URL, params=params, timeout=30)
    resp.raise_for_status()

    sightings = []
    for obs in resp.json().get("results", []):
        if not obs.get("geojson") and not obs.get("location"):
            continue

        # Get coordinates
        if obs.get("location"):
            lat_s, lon_s = obs["location"].split(",")
            olat, olon = float(lat_s), float(lon_s)
        else:
            olon, olat = obs["geojson"]["coordinates"]

        # Get photo
        photo = None
        photos = obs.get("photos", [])
        if photos and photos[0].get("url"):
            photo = photos[0]["url"].replace("/square.", "/small.")

        sightings.append({
            "id": obs["id"],
            "date": obs.get("observed_on"),
            "lat": round(olat, 4),
            "lon": round(olon, 4),
            "photo": photo,
            "uri": obs.get("uri"),
        })

    return sightings


def update_all_regions():
    """Fetch indicator sightings for all regions."""
    output = {}

    for region_key, region in REGION_INDICATORS.items():
        print(f"\n  {region['name']}:")
        region_data = {
            "name": region["name"],
            "taxa": [],
        }

        for taxon in region["taxa"]:
            try:
                sightings = fetch_recent_sightings(
                    taxon["id"],
                    region["center"][0],
                    region["center"][1],
                    region["radius_km"],
                )
                print(f"    {taxon['common']}: {len(sightings)} sightings this season")

                region_data["taxa"].append({
                    "id": taxon["id"],
                    "name": taxon["name"],
                    "common": taxon["common"],
                    "why": taxon["why"],
                    "icon": taxon["icon"],
                    "sightings": sightings,
                })

                time.sleep(1.2)  # respect iNaturalist rate limits

            except Exception as e:
                print(f"    {taxon['common']}: ERROR — {e}")
                region_data["taxa"].append({
                    "id": taxon["id"],
                    "name": taxon["name"],
                    "common": taxon["common"],
                    "why": taxon["why"],
                    "icon": taxon["icon"],
                    "sightings": [],
                })
                time.sleep(2)

        output[region_key] = region_data

    return output


def main():
    print(f"Updating indicator species at {datetime.now(timezone.utc).isoformat()}")

    data = update_all_regions()

    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "regions": data,
    }

    outfile = APP_DATA / "indicators-latest.json"
    with open(outfile, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = outfile.stat().st_size / 1024
    total_sightings = sum(
        len(t["sightings"])
        for r in data.values()
        for t in r["taxa"]
    )
    print(f"\nDone!")
    print(f"  Total sightings: {total_sightings}")
    print(f"  Output: {outfile} ({size_kb:.0f} KB)")


if __name__ == "__main__":
    main()
