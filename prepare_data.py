"""
Prepare data files for the morel prediction web app.

Transforms our raw 49K sighting dataset into compact, web-ready formats:
1. Regional GeoJSON files (MA/New England sightings)
2. Density grid (0.1° cells with sighting counts and peak timing)
3. Weather fetch grid (cell centroids for live API calls)
4. Indicator species config (iNaturalist taxon IDs)
"""

import json
from pathlib import Path

import pandas as pd
import numpy as np

DATA = Path("data")
APP_DATA = Path("app/data")
APP_DATA.mkdir(parents=True, exist_ok=True)

# Bounding boxes
REGIONS = {
    "ma": {"name": "Massachusetts", "lat_min": 41.15, "lat_max": 42.9, "lon_min": -73.55, "lon_max": -69.85},
    "ne": {"name": "New England", "lat_min": 40.9, "lat_max": 47.5, "lon_min": -73.8, "lon_max": -66.9},
}

# Grid resolution for weather fetching and density calculation
GRID_RES = 0.1  # degrees (~11km)

# Indicator species with iNaturalist taxon IDs
# These are the top co-occurring plant species from our analysis,
# plus traditional phenological indicators for morel timing.
INDICATOR_TAXA = [
    {"taxon_id": 60714, "name": "Alliaria petiolata", "common_name": "Garlic Mustard",
     "why": "Top co-occurring species in our data. Thrives in same moist deciduous habitat as morels.",
     "color": "#4CAF50", "icon": "leaf"},
    {"taxon_id": 56955, "name": "Podophyllum peltatum", "common_name": "Mayapple",
     "why": "Classic morel indicator. When mayapple leaves unfurl, morels are fruiting.",
     "color": "#8BC34A", "icon": "leaf"},
    {"taxon_id": 56077, "name": "Erythronium americanum", "common_name": "Yellow Trout Lily",
     "why": "Spring ephemeral that blooms during morel season in deciduous forests.",
     "color": "#FFEB3B", "icon": "flower"},
    {"taxon_id": 47585, "name": "Sanguinaria canadensis", "common_name": "Bloodroot",
     "why": "Early spring bloomer in rich deciduous woods — same habitat as morels.",
     "color": "#F5F5F5", "icon": "flower"},
    {"taxon_id": 49409, "name": "Mertensia virginica", "common_name": "Virginia Bluebells",
     "why": "Blooms in floodplain forests during peak morel window.",
     "color": "#2196F3", "icon": "flower"},
    {"taxon_id": 49968, "name": "Claytonia virginica", "common_name": "Spring Beauty",
     "why": "Carpets forest floors right as morels start appearing.",
     "color": "#E91E63", "icon": "flower"},
    {"taxon_id": 47599, "name": "Trillium", "common_name": "Trillium",
     "why": "Traditional indicator — 'when trillium blooms, look for morels.'",
     "color": "#9C27B0", "icon": "flower"},
    {"taxon_id": 50310, "name": "Dicentra cucullaria", "common_name": "Dutchman's Breeches",
     "why": "Early spring ephemeral in rich woods. Blooms just before peak morel time.",
     "color": "#FAFAFA", "icon": "flower"},
    {"taxon_id": 77250, "name": "Syringa vulgaris", "common_name": "Common Lilac",
     "why": "Phenological benchmark — lilac bloom correlates with morel timing across regions.",
     "color": "#CE93D8", "icon": "flower"},
    {"taxon_id": 48978, "name": "Taraxacum officinale", "common_name": "Dandelion",
     "why": "When dandelions are blooming, soil temps are in the morel range.",
     "color": "#FFC107", "icon": "flower"},
]

# Morel probability scoring thresholds (from our validated analysis)
SCORING_CONFIG = {
    "soil_temp": {
        "unit": "celsius",
        "optimal": 12.0,
        "sigma": 3.5,
        "weight": 0.35,
        "description": "Soil temp at 6cm depth, 7-day mean. Sweet spot: 45-61°F (7-16°C), peak at 52°F (11.4°C)."
    },
    "precipitation_14d": {
        "unit": "mm",
        "optimal": 35.0,
        "sigma": 15.0,
        "weight": 0.25,
        "description": "Total precipitation in prior 14 days. Sweet spot: 1-2 inches (25-50mm)."
    },
    "historical_density": {
        "weight": 0.25,
        "description": "Normalized count of historical morel sightings in this grid cell."
    },
    "forest_cover": {
        "weight": 0.15,
        "description": "Whether the area is deciduous/mixed forest (NLCD classes 41, 43)."
    },
}

# Honorable Harvest guidelines (Robin Wall Kimmerer, Braiding Sweetgrass)
HONORABLE_HARVEST = {
    "title": "The Honorable Harvest",
    "attribution": "Guidelines inspired by Robin Wall Kimmerer's Braiding Sweetgrass and Indigenous harvesting traditions.",
    "principles": [
        {"text": "Know the ways of the ones who take care of you, so that you may take care of them."},
        {"text": "Ask permission before taking. Listen for the answer."},
        {"text": "Never take the first. Never take the last."},
        {"text": "Take only what you need."},
        {"text": "Take only that which is given."},
        {"text": "Never take more than half. Leave some for others."},
        {"text": "Harvest in a way that minimizes harm."},
        {"text": "Use everything you take."},
        {"text": "Share."},
        {"text": "Give thanks for what you have been given."},
        {"text": "Give a gift in reciprocity for what you have taken."},
        {"text": "Sustain the ones who sustain you, and the earth will last forever."},
    ],
    "foraging_tips": [
        "Cut morels at the base with a knife — don't pull them up by the roots, which disturbs the mycelium.",
        "Carry your harvest in a mesh bag so spores can spread as you walk.",
        "If you find a big patch, leave most of it. Come back tomorrow — they grow fast.",
        "Share your knowledge freely, but be thoughtful about sharing exact locations.",
        "Teach someone new. The best way to sustain foraging culture is to pass it on.",
    ],
}


def load_sightings():
    """Load and clean the raw sightings data."""
    print("Loading sightings data...")
    df = pd.read_csv(DATA / "morel_sightings.csv", parse_dates=["observed_on"])
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude", "observed_on"])
    df["year"] = df["observed_on"].dt.year
    df["month"] = df["observed_on"].dt.month
    df["day_of_year"] = df["observed_on"].dt.dayofyear
    print(f"  Loaded {len(df):,} observations with coordinates")
    return df


def make_regional_geojson(df, region_key):
    """Create a compact GeoJSON file for a region's sightings."""
    region = REGIONS[region_key]
    mask = (
        (df["latitude"] >= region["lat_min"]) & (df["latitude"] <= region["lat_max"]) &
        (df["longitude"] >= region["lon_min"]) & (df["longitude"] <= region["lon_max"])
    )
    regional = df[mask].copy()
    print(f"  {region['name']}: {len(regional):,} sightings")

    features = []
    for _, row in regional.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [round(row["longitude"], 5), round(row["latitude"], 5)]
            },
            "properties": {
                "id": int(row["id"]),
                "date": row["observed_on"].strftime("%Y-%m-%d"),
                "year": int(row["year"]),
                "month": int(row["month"]),
                "doy": int(row["day_of_year"]),
                "species": row["taxon_name"] if pd.notna(row["taxon_name"]) else "Morchella",
                "common": row["taxon_common_name"] if pd.notna(row["taxon_common_name"]) else None,
                "quality": row["quality_grade"],
                "photo": row["photo_url"] if pd.notna(row["photo_url"]) else None,
                "uri": row["uri"] if pd.notna(row["uri"]) else None,
            }
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    outfile = APP_DATA / f"sightings-{region_key}.geojson"
    with open(outfile, "w") as f:
        json.dump(geojson, f, separators=(",", ":"))  # compact
    size_kb = outfile.stat().st_size / 1024
    print(f"  Wrote {outfile} ({size_kb:.0f} KB, {len(features)} features)")
    return regional


def make_density_grid(df, grid_res=GRID_RES):
    """Aggregate all sightings into grid cells for the heatmap/probability layer."""
    print("Building density grid...")

    # Round lat/lon to grid resolution
    df = df.copy()
    df["grid_lat"] = (df["latitude"] / grid_res).round() * grid_res
    df["grid_lon"] = (df["longitude"] / grid_res).round() * grid_res

    # Aggregate per cell
    cells = df.groupby(["grid_lat", "grid_lon"]).agg(
        count=("id", "size"),
        peak_month=("month", lambda x: x.mode().iloc[0] if len(x) > 0 else None),
        peak_doy=("day_of_year", "median"),
        earliest_doy=("day_of_year", lambda x: x.quantile(0.1)),
        latest_doy=("day_of_year", lambda x: x.quantile(0.9)),
        species_count=("taxon_name", "nunique"),
        top_species=("taxon_name", lambda x: x.mode().iloc[0] if len(x) > 0 else None),
    ).reset_index()

    # Normalize count to 0-1 for scoring
    max_count = cells["count"].max()
    cells["density_score"] = (cells["count"] / max_count).round(4)

    grid_data = []
    for _, row in cells.iterrows():
        grid_data.append({
            "lat": round(row["grid_lat"], 2),
            "lon": round(row["grid_lon"], 2),
            "count": int(row["count"]),
            "density": round(row["density_score"], 4),
            "peak_month": int(row["peak_month"]) if pd.notna(row["peak_month"]) else None,
            "peak_doy": int(row["peak_doy"]) if pd.notna(row["peak_doy"]) else None,
            "earliest_doy": int(row["earliest_doy"]) if pd.notna(row["earliest_doy"]) else None,
            "latest_doy": int(row["latest_doy"]) if pd.notna(row["latest_doy"]) else None,
            "species": int(row["species_count"]),
            "top": row["top_species"],
        })

    outfile = APP_DATA / "density-grid.json"
    with open(outfile, "w") as f:
        json.dump(grid_data, f, separators=(",", ":"))
    size_kb = outfile.stat().st_size / 1024
    print(f"  Wrote {outfile} ({size_kb:.0f} KB, {len(grid_data)} cells)")
    print(f"  Max sightings in one cell: {max_count}")
    print(f"  Cells with >10 sightings: {len(cells[cells['count'] > 10])}")


def make_weather_grid(region_key):
    """Generate grid centroids for live weather API fetching."""
    region = REGIONS[region_key]

    lats = np.arange(region["lat_min"], region["lat_max"] + GRID_RES, GRID_RES)
    lons = np.arange(region["lon_min"], region["lon_max"] + GRID_RES, GRID_RES)

    grid = []
    for lat in lats:
        for lon in lons:
            grid.append({
                "lat": round(float(lat), 2),
                "lon": round(float(lon), 2),
            })

    outfile = APP_DATA / f"grid-{region_key}.json"
    with open(outfile, "w") as f:
        json.dump(grid, f, separators=(",", ":"))
    print(f"  Wrote {outfile} ({len(grid)} cells covering {region['name']})")
    return grid


def make_indicator_taxa():
    """Write the indicator species config."""
    outfile = APP_DATA / "indicator-taxa.json"
    with open(outfile, "w") as f:
        json.dump(INDICATOR_TAXA, f, indent=2)
    print(f"  Wrote {outfile} ({len(INDICATOR_TAXA)} indicator species)")


def make_scoring_config():
    """Write the probability scoring configuration."""
    outfile = APP_DATA / "scoring-config.json"
    with open(outfile, "w") as f:
        json.dump(SCORING_CONFIG, f, indent=2)
    print(f"  Wrote {outfile}")


def make_honorable_harvest():
    """Write the Honorable Harvest guidelines."""
    outfile = APP_DATA / "honorable-harvest.json"
    with open(outfile, "w") as f:
        json.dump(HONORABLE_HARVEST, f, indent=2)
    print(f"  Wrote {outfile}")


def make_app_config():
    """Write the master app configuration."""
    config = {
        "regions": REGIONS,
        "default_region": "ma",
        "default_center": [42.2793, -71.4162],  # Framingham, MA
        "default_zoom": 9,
        "grid_resolution": GRID_RES,
        "morel_taxon_name": "Morchella",
        "morel_taxon_id": 56830,
        "apis": {
            "open_meteo_forecast": "https://api.open-meteo.com/v1/forecast",
            "inaturalist_observations": "https://api.inaturalist.org/v1/observations",
            "inaturalist_species_counts": "https://api.inaturalist.org/v1/observations/species_counts",
            "massgis_openspace": "https://gis.eea.mass.gov/server/rest/services/Protected_and_Recreational_OpenSpace_Polygons/FeatureServer/0/query",
            "nlcd_wms": "https://www.mrlc.gov/geoserver/mrlc_display/NLCD_2021_Land_Cover_L48/wms",
        },
        "soil_temp_thresholds": {
            "too_cold": 7,
            "warming": 10,
            "sweet_spot_low": 10,
            "optimal": 12,
            "sweet_spot_high": 16,
            "too_hot": 20,
            "unit": "celsius",
        },
        "precip_thresholds": {
            "dry": 15,
            "good_low": 25,
            "optimal": 35,
            "good_high": 50,
            "saturated": 75,
            "window_days": 14,
            "unit": "mm",
        },
    }

    outfile = APP_DATA / "app-config.json"
    with open(outfile, "w") as f:
        json.dump(config, f, indent=2)
    print(f"  Wrote {outfile}")


def print_summary(df, ma_sightings):
    """Print a summary of what was generated."""
    print(f"\n{'='*60}")
    print(f"  DATA PREP COMPLETE")
    print(f"{'='*60}")
    print(f"\n  Source: {len(df):,} total sightings")
    print(f"  MA sightings: {len(ma_sightings):,}")
    print(f"\n  Files generated in {APP_DATA}/:")
    for f in sorted(APP_DATA.glob("*")):
        size = f.stat().st_size
        if size > 1024:
            print(f"    {f.name} ({size/1024:.0f} KB)")
        else:
            print(f"    {f.name} ({size} B)")

    # Quick stats on the MA data
    if len(ma_sightings) > 0:
        rg = ma_sightings[ma_sightings["quality_grade"] == "research"]
        print(f"\n  MA sighting stats:")
        print(f"    Research-grade: {len(rg):,}")
        print(f"    Date range: {ma_sightings['observed_on'].min().date()} — {ma_sightings['observed_on'].max().date()}")
        print(f"    Unique species: {ma_sightings['taxon_name'].nunique()}")
        print(f"    Peak month: {ma_sightings['month'].mode().iloc[0]} "
              f"({'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[ma_sightings['month'].mode().iloc[0]-1]})")


def main():
    df = load_sightings()

    print("\n--- Regional GeoJSON ---")
    ma_sightings = make_regional_geojson(df, "ma")
    make_regional_geojson(df, "ne")

    print("\n--- Density Grid (all sightings) ---")
    make_density_grid(df)

    print("\n--- Weather Fetch Grid ---")
    make_weather_grid("ma")

    print("\n--- Indicator Species Config ---")
    make_indicator_taxa()

    print("\n--- Scoring Config ---")
    make_scoring_config()

    print("\n--- Honorable Harvest Guidelines ---")
    make_honorable_harvest()

    print("\n--- App Config ---")
    make_app_config()

    print_summary(df, ma_sightings)


if __name__ == "__main__":
    main()
