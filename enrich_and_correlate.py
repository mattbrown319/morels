"""
Enrich morel sightings with environmental data and run correlation analysis.

Data sources:
- Open-Meteo: historical weather, soil temp/moisture, elevation
- UNEP-WCMC: ecoregion/biome classification
- ISRIC SoilGrids: soil composition (clay, sand, pH, organic carbon)
"""

import csv
import json
import time
import random
import sys
from pathlib import Path
from collections import Counter

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import requests

sns.set_theme(style="whitegrid")
OUT = Path("analysis")
OUT.mkdir(exist_ok=True)
DATA = Path("data")

SAMPLE_SIZE = 2500  # observations to enrich with weather
WEATHER_LOOKBACK_DAYS = 30  # days before sighting to pull weather


# ---------------------------------------------------------------------------
# 1. ELEVATION (fast — batches of 100)
# ---------------------------------------------------------------------------
def fetch_elevation_batch(lats, lons):
    """Fetch elevation for up to 100 points at once."""
    url = "https://api.open-meteo.com/v1/elevation"
    params = {
        "latitude": ",".join(str(x) for x in lats),
        "longitude": ",".join(str(x) for x in lons),
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()["elevation"]


def enrich_elevation(df):
    """Add elevation using grid-based caching to reduce API calls dramatically."""
    cache_file = DATA / "elevation_grid_cache.json"
    cache = {}
    if cache_file.exists():
        with open(cache_file) as f:
            cache = json.load(f)
        print(f"  Loaded {len(cache)} cached elevation grid cells")

    # Round to 0.05 degree grid (~5km) — elevation doesn't change much at this scale
    df["elev_key"] = (
        (df["latitude"] * 20).round() / 20
    ).astype(str) + "," + (
        (df["longitude"] * 20).round() / 20
    ).astype(str)

    unique_keys = list(df["elev_key"].unique())
    need = [k for k in unique_keys if k not in cache]
    print(f"  Elevation: {len(unique_keys)} unique grid cells, {len(need)} need fetching")

    # Batch the grid cell lookups
    batch_size = 50  # smaller batches to avoid 429s
    for i in range(0, len(need), batch_size):
        batch_keys = need[i:i+batch_size]
        lats = [float(k.split(",")[0]) for k in batch_keys]
        lons = [float(k.split(",")[1]) for k in batch_keys]

        try:
            elevs = fetch_elevation_batch(lats, lons)
            for key, elev in zip(batch_keys, elevs):
                cache[key] = elev
        except Exception as e:
            print(f"\n  Error at batch {i}: {e}")
            time.sleep(15)
            try:
                elevs = fetch_elevation_batch(lats, lons)
                for key, elev in zip(batch_keys, elevs):
                    cache[key] = elev
            except Exception:
                continue

        done = min(i + batch_size, len(need))
        print(f"\r  Elevation: {done:,}/{len(need):,} grid cells", end="", flush=True)
        time.sleep(1.5)  # very conservative

        if done % 500 == 0:
            with open(cache_file, "w") as f:
                json.dump(cache, f)

    print()

    with open(cache_file, "w") as f:
        json.dump(cache, f)

    df["elevation_m"] = df["elev_key"].map(lambda k: cache.get(k))
    df = df.drop(columns=["elev_key"])

    print()

    # Save cache
    elev_df = pd.DataFrame([
        {"id": k, "elevation_m": v} for k, v in elevations.items()
    ])
    elev_df.to_csv(cache_file, index=False)

    df["elevation_m"] = df["id"].map(elevations)
    return df


# ---------------------------------------------------------------------------
# 2. WEATHER (sampled — 1 call per observation, 30-day lookback)
# ---------------------------------------------------------------------------
def fetch_weather(lat, lon, end_date, lookback_days=30):
    """Fetch daily weather + soil data for the period before a sighting."""
    start = (pd.Timestamp(end_date) - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end = pd.Timestamp(end_date).strftime("%Y-%m-%d")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": ",".join([
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "rain_sum",
        ]),
        "hourly": ",".join([
            "soil_temperature_0_to_7cm", "soil_moisture_0_to_7cm",
        ]),
        "timezone": "auto",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    daily = data.get("daily", {})
    hourly = data.get("hourly", {})

    # Compute summary stats from the 30-day window
    def safe_mean(vals):
        clean = [v for v in (vals or []) if v is not None]
        return np.mean(clean) if clean else None

    def safe_sum(vals):
        clean = [v for v in (vals or []) if v is not None]
        return np.sum(clean) if clean else None

    # Last 7 days (most recent conditions)
    def last_n(vals, n):
        if not vals:
            return []
        return [v for v in vals[-n:] if v is not None]

    return {
        "temp_mean_30d": safe_mean(daily.get("temperature_2m_mean")),
        "temp_max_30d": safe_mean(daily.get("temperature_2m_max")),
        "temp_min_30d": safe_mean(daily.get("temperature_2m_min")),
        "precip_total_30d": safe_sum(daily.get("precipitation_sum")),
        "precip_total_7d": safe_sum(last_n(daily.get("precipitation_sum"), 7)),
        "precip_total_14d": safe_sum(last_n(daily.get("precipitation_sum"), 14)),
        "temp_mean_7d": safe_mean(last_n(daily.get("temperature_2m_mean"), 7)),
        "temp_mean_14d": safe_mean(last_n(daily.get("temperature_2m_mean"), 14)),
        "soil_temp_mean_30d": safe_mean(hourly.get("soil_temperature_0_to_7cm")),
        "soil_temp_mean_7d": safe_mean(
            (hourly.get("soil_temperature_0_to_7cm") or [])[-168:]  # 7 days * 24 hrs
        ),
        "soil_moisture_mean_30d": safe_mean(hourly.get("soil_moisture_0_to_7cm")),
        "soil_moisture_mean_7d": safe_mean(
            (hourly.get("soil_moisture_0_to_7cm") or [])[-168:]
        ),
    }


def enrich_weather(df, sample_size=SAMPLE_SIZE):
    """Add weather data to a sample of observations."""
    cache_file = DATA / "weather_cache.json"
    cache = {}
    if cache_file.exists():
        with open(cache_file) as f:
            cache = json.load(f)
        print(f"  Loaded {len(cache)} cached weather records")

    # Sample research-grade observations, stratified by year
    rg = df[df["quality_grade"] == "research"].copy()
    # Remove observations before 2010 (ERA5-Land coverage is better)
    rg = rg[rg["year"] >= 2010]

    # Stratified sample by year
    samples = []
    for year, group in rg.groupby("year"):
        n = min(len(group), max(50, int(sample_size * len(group) / len(rg))))
        samples.append(group.sample(n=n, random_state=42))
    sample = pd.concat(samples)

    # Remove already-cached
    need = sample[~sample["id"].astype(str).isin(cache)]
    print(f"  Weather sample: {len(sample):,} observations, {len(need):,} need fetching")

    for i, (_, row) in enumerate(need.iterrows()):
        obs_id = str(row["id"])
        try:
            weather = fetch_weather(row["latitude"], row["longitude"], row["observed_on"])
            cache[obs_id] = weather
        except Exception as e:
            print(f"\n  Error for obs {obs_id}: {e}")
            time.sleep(5)
            continue

        if (i + 1) % 10 == 0:
            print(f"\r  Weather: {i+1:,}/{len(need):,}", end="", flush=True)

        time.sleep(0.25)  # ~4 req/s, well within 600/min

        # Save cache periodically
        if (i + 1) % 200 == 0:
            with open(cache_file, "w") as f:
                json.dump(cache, f)

    print()

    # Save final cache
    with open(cache_file, "w") as f:
        json.dump(cache, f)

    # Merge weather data into sample
    weather_df = pd.DataFrame([
        {"id": int(k), **v} for k, v in cache.items()
    ])
    sample = sample.merge(weather_df, on="id", how="left")
    return sample


# ---------------------------------------------------------------------------
# 3. ECOREGION / BIOME
# ---------------------------------------------------------------------------
def fetch_ecoregion(lat, lon):
    """Look up ecoregion for a lat/lon point."""
    url = ("https://data-gis.unep-wcmc.org/server/rest/services/"
           "Bio-geographicalRegions/Resolve_Ecoregions/FeatureServer/0/query")
    params = {
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": 4326,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "eco_name,biome_name,realm",
        "f": "json",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", [])
    if features:
        attrs = features[0].get("attributes", {})
        return {
            "ecoregion": attrs.get("eco_name"),
            "biome": attrs.get("biome_name"),
            "realm": attrs.get("realm"),
        }
    return {"ecoregion": None, "biome": None, "realm": None}


def enrich_ecoregion(df):
    """Add ecoregion data. Use grid-based caching to avoid redundant lookups."""
    cache_file = DATA / "ecoregion_cache.json"
    cache = {}
    if cache_file.exists():
        with open(cache_file) as f:
            cache = json.load(f)
        print(f"  Loaded {len(cache)} cached ecoregion lookups")

    # Round to 0.5 degree grid for caching (ecoregions don't change at fine scale)
    df["eco_key"] = (
        (df["latitude"] * 2).round() / 2
    ).astype(str) + "," + (
        (df["longitude"] * 2).round() / 2
    ).astype(str)

    unique_keys = df["eco_key"].unique()
    need = [k for k in unique_keys if k not in cache]
    print(f"  Ecoregion: {len(unique_keys)} unique grid cells, {len(need)} need fetching")

    for i, key in enumerate(need):
        lat, lon = key.split(",")
        try:
            result = fetch_ecoregion(float(lat), float(lon))
            cache[key] = result
        except Exception as e:
            print(f"\n  Error for {key}: {e}")
            cache[key] = {"ecoregion": None, "biome": None, "realm": None}
            time.sleep(2)
            continue

        if (i + 1) % 10 == 0:
            print(f"\r  Ecoregion: {i+1:,}/{len(need):,}", end="", flush=True)

        time.sleep(0.3)

        if (i + 1) % 100 == 0:
            with open(cache_file, "w") as f:
                json.dump(cache, f)

    print()

    with open(cache_file, "w") as f:
        json.dump(cache, f)

    df["ecoregion"] = df["eco_key"].map(lambda k: (cache.get(k) or {}).get("ecoregion"))
    df["biome"] = df["eco_key"].map(lambda k: (cache.get(k) or {}).get("biome"))
    df["realm"] = df["eco_key"].map(lambda k: (cache.get(k) or {}).get("realm"))
    df = df.drop(columns=["eco_key"])
    return df


# ---------------------------------------------------------------------------
# 4. SOIL COMPOSITION (small sample due to rate limits)
# ---------------------------------------------------------------------------
def fetch_soil(lat, lon):
    """Fetch soil properties from ISRIC SoilGrids."""
    url = "https://rest.isric.org/soilgrids/v2.0/properties/query"
    params = {
        "lon": lon,
        "lat": lat,
        "property": ["clay", "sand", "silt", "phh2o", "soc"],
        "depth": "0-5cm",
        "value": "mean",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    result = {}
    for layer in data.get("properties", {}).get("layers", []):
        prop_name = layer["name"]
        depths = layer.get("depths", [])
        if depths:
            val = depths[0].get("values", {}).get("mean")
            # SoilGrids returns values * 10 for clay/sand/silt (g/kg)
            # and * 10 for pH
            if prop_name in ("clay", "sand", "silt"):
                result[f"soil_{prop_name}_pct"] = val / 10 if val is not None else None
            elif prop_name == "phh2o":
                result["soil_ph"] = val / 10 if val is not None else None
            elif prop_name == "soc":
                result["soil_organic_carbon_gkg"] = val / 10 if val is not None else None
    return result


def enrich_soil(df, sample_size=150):
    """Add soil data to a subsample (API is rate-limited to 5/min)."""
    cache_file = DATA / "soil_cache.json"
    cache = {}
    if cache_file.exists():
        with open(cache_file) as f:
            cache = json.load(f)
        print(f"  Loaded {len(cache)} cached soil records")

    # Grid-based caching at 0.25 degree resolution
    df["soil_key"] = (
        (df["latitude"] * 4).round() / 4
    ).astype(str) + "," + (
        (df["longitude"] * 4).round() / 4
    ).astype(str)

    unique_keys = list(df["soil_key"].unique())
    random.shuffle(unique_keys)
    need = [k for k in unique_keys if k not in cache][:sample_size]
    print(f"  Soil: {len(unique_keys)} unique grid cells, fetching up to {len(need)}")

    errors = 0
    for i, key in enumerate(need):
        lat, lon = key.split(",")
        try:
            result = fetch_soil(float(lat), float(lon))
            cache[key] = result
            errors = 0  # reset on success
        except Exception as e:
            errors += 1
            cache[key] = {}
            if errors >= 5:
                print(f"\n  Too many consecutive errors, stopping soil fetch. Last: {e}")
                break
            time.sleep(5)
            continue

        if (i + 1) % 5 == 0:
            print(f"\r  Soil: {i+1:,}/{len(need):,}", end="", flush=True)

        time.sleep(12.5)  # 5 per minute limit

        if (i + 1) % 20 == 0:
            with open(cache_file, "w") as f:
                json.dump(cache, f)

    print()

    with open(cache_file, "w") as f:
        json.dump(cache, f)

    for prop in ["soil_clay_pct", "soil_sand_pct", "soil_silt_pct", "soil_ph", "soil_organic_carbon_gkg"]:
        df[prop] = df["soil_key"].map(lambda k, p=prop: (cache.get(k) or {}).get(p))

    df = df.drop(columns=["soil_key"])
    return df


# ---------------------------------------------------------------------------
# 5. CO-OCCURRING SPECIES from iNaturalist
# ---------------------------------------------------------------------------
def fetch_nearby_species(lat, lon, date, radius_km=5, days_window=30):
    """Find other species observed near a morel sighting."""
    start = (pd.Timestamp(date) - pd.Timedelta(days=days_window)).strftime("%Y-%m-%d")
    end = pd.Timestamp(date).strftime("%Y-%m-%d")

    url = "https://api.inaturalist.org/v1/observations/species_counts"
    params = {
        "lat": lat,
        "lng": lon,
        "radius": radius_km,
        "d1": start,
        "d2": end,
        "quality_grade": "research",
        "per_page": 50,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    results = resp.json().get("results", [])
    species = []
    for r in results:
        taxon = r.get("taxon", {})
        species.append({
            "name": taxon.get("name"),
            "common_name": taxon.get("preferred_common_name"),
            "rank": taxon.get("rank"),
            "iconic_taxon": taxon.get("iconic_taxon_name"),
            "count": r.get("count"),
        })
    return species


def analyze_co_occurring_species(df, n_samples=150):
    """Sample morel observations and find what other species occur nearby."""
    cache_file = DATA / "cospecies_cache.json"
    cache = {}
    if cache_file.exists():
        with open(cache_file) as f:
            cache = json.load(f)
        print(f"  Loaded {len(cache)} cached co-species lookups")

    rg = df[(df["quality_grade"] == "research") & (df["year"] >= 2018)]
    sample = rg.sample(n=min(n_samples, len(rg)), random_state=42)

    need = sample[~sample["id"].astype(str).isin(cache)]
    print(f"  Co-occurring species: {len(sample)} sampled, {len(need)} need fetching")

    for i, (_, row) in enumerate(need.iterrows()):
        obs_id = str(row["id"])
        try:
            species = fetch_nearby_species(row["latitude"], row["longitude"], row["observed_on"])
            cache[obs_id] = species
        except Exception as e:
            print(f"\n  Error for {obs_id}: {e}")
            time.sleep(5)
            continue

        if (i + 1) % 10 == 0:
            print(f"\r  Co-species: {i+1:,}/{len(need):,}", end="", flush=True)

        time.sleep(1.0)  # iNaturalist rate limit

        if (i + 1) % 50 == 0:
            with open(cache_file, "w") as f:
                json.dump(cache, f)

    print()

    with open(cache_file, "w") as f:
        json.dump(cache, f)

    return cache


# ---------------------------------------------------------------------------
# ANALYSIS & VISUALIZATION
# ---------------------------------------------------------------------------
def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def analyze_elevation(df):
    section("ELEVATION ANALYSIS")
    elev = df["elevation_m"].dropna()
    print(f"  Observations with elevation: {len(elev):,}")
    print(f"  Mean:   {elev.mean():.0f} m ({elev.mean() * 3.281:.0f} ft)")
    print(f"  Median: {elev.median():.0f} m ({elev.median() * 3.281:.0f} ft)")
    print(f"  Mode:   {elev.mode().iloc[0]:.0f} m")
    print(f"  Std:    {elev.std():.0f} m")
    print(f"  Min:    {elev.min():.0f} m")
    print(f"  Max:    {elev.max():.0f} m")
    print(f"  25th %%: {elev.quantile(0.25):.0f} m")
    print(f"  75th %%: {elev.quantile(0.75):.0f} m")

    # Elevation by species
    top_species = df["taxon_name"].value_counts().head(8).index
    print(f"\n  Median elevation by species:")
    for sp in top_species:
        sp_elev = df[df["taxon_name"] == sp]["elevation_m"].dropna()
        if len(sp_elev) > 10:
            print(f"    {sp}: {sp_elev.median():.0f} m ({sp_elev.median()*3.281:.0f} ft) [n={len(sp_elev)}]")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    axes[0].hist(elev[elev < 3000], bins=60, color="sienna", alpha=0.7, edgecolor="white")
    axes[0].set_xlabel("Elevation (m)")
    axes[0].set_ylabel("Count")
    axes[0].set_title("Elevation Distribution of Morel Sightings")
    axes[0].axvline(elev.median(), color="red", linestyle="--", label=f"Median: {elev.median():.0f}m")
    axes[0].legend()

    # Elevation vs timing
    spring = df[(df["month"].isin([3,4,5,6])) & (df["latitude"] > 30) & (df["latitude"] < 50)]
    spring_elev = spring.dropna(subset=["elevation_m"])
    axes[1].scatter(spring_elev["elevation_m"], spring_elev["day_of_year"],
                    s=2, alpha=0.2, c="sienna")
    axes[1].set_xlabel("Elevation (m)")
    axes[1].set_ylabel("Day of Year")
    axes[1].set_title("Elevation vs. Fruiting Timing (Spring, N. Hemisphere)")
    plt.tight_layout()
    plt.savefig(OUT / "elevation_analysis.png", dpi=150)
    plt.close()


def analyze_weather(weather_df):
    section("WEATHER CORRELATION ANALYSIS")

    # Check which weather columns we have
    weather_cols = [c for c in weather_df.columns if c.startswith(("temp_", "precip_", "soil_t", "soil_m"))]
    print(f"  Weather-enriched observations: {len(weather_df):,}")
    print(f"  Weather variables: {len(weather_cols)}")

    print(f"\n  Weather conditions at time of morel sighting:")
    for col in weather_cols:
        vals = weather_df[col].dropna()
        if len(vals) > 10:
            unit = "°C" if "temp" in col else "mm" if "precip" in col else "m³/m³" if "moisture" in col else ""
            print(f"    {col}:")
            print(f"      Mean: {vals.mean():.1f}{unit}  Median: {vals.median():.1f}{unit}  Std: {vals.std():.1f}")
            print(f"      25th: {vals.quantile(0.25):.1f}  75th: {vals.quantile(0.75):.1f}")

    # Correlation matrix
    corr_cols = weather_cols + ["day_of_year", "latitude", "elevation_m"]
    corr_cols = [c for c in corr_cols if c in weather_df.columns]
    corr_data = weather_df[corr_cols].dropna()

    if len(corr_data) > 50:
        corr_matrix = corr_data.corr()

        fig, ax = plt.subplots(figsize=(14, 10))
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)
        sns.heatmap(corr_matrix, mask=mask, annot=True, fmt=".2f", cmap="RdBu_r",
                    center=0, ax=ax, square=True, linewidths=0.5)
        ax.set_title("Correlation Matrix: Weather & Environmental Variables at Morel Sightings")
        plt.tight_layout()
        plt.savefig(OUT / "weather_correlation_matrix.png", dpi=150)
        plt.close()

        # Key correlations with day_of_year
        print(f"\n  Correlations with fruiting day (day_of_year):")
        doy_corr = corr_matrix["day_of_year"].drop("day_of_year").sort_values()
        for var, r in doy_corr.items():
            strength = "strong" if abs(r) > 0.4 else "moderate" if abs(r) > 0.2 else "weak"
            print(f"    {var}: r={r:.3f} ({strength})")

    # Soil temperature sweet spot
    if "soil_temp_mean_7d" in weather_df.columns:
        soil_temp = weather_df["soil_temp_mean_7d"].dropna()
        if len(soil_temp) > 50:
            print(f"\n  SOIL TEMPERATURE at time of fruiting (7-day mean, 0-7cm depth):")
            print(f"    Mean:   {soil_temp.mean():.1f}°C ({soil_temp.mean() * 9/5 + 32:.1f}°F)")
            print(f"    Median: {soil_temp.median():.1f}°C ({soil_temp.median() * 9/5 + 32:.1f}°F)")
            print(f"    Mode band: {soil_temp.round().mode().iloc[0]:.0f}°C")
            print(f"    10th %%: {soil_temp.quantile(0.10):.1f}°C ({soil_temp.quantile(0.10) * 9/5 + 32:.1f}°F)")
            print(f"    90th %%: {soil_temp.quantile(0.90):.1f}°C ({soil_temp.quantile(0.90) * 9/5 + 32:.1f}°F)")

            fig, ax = plt.subplots(figsize=(10, 5))
            ax.hist(soil_temp, bins=40, color="sienna", alpha=0.7, edgecolor="white")
            ax.axvline(soil_temp.median(), color="red", linestyle="--",
                       label=f"Median: {soil_temp.median():.1f}°C / {soil_temp.median()*9/5+32:.0f}°F")
            ax.set_xlabel("Soil Temperature 0-7cm (°C)")
            ax.set_ylabel("Count")
            ax.set_title("Soil Temperature at Time of Morel Fruiting (7-day mean)")
            ax.legend()
            plt.tight_layout()
            plt.savefig(OUT / "soil_temp_distribution.png", dpi=150)
            plt.close()

    # Precipitation patterns
    if "precip_total_30d" in weather_df.columns:
        precip = weather_df["precip_total_30d"].dropna()
        if len(precip) > 50:
            print(f"\n  PRECIPITATION in 30 days before fruiting:")
            print(f"    Mean:   {precip.mean():.1f} mm ({precip.mean() / 25.4:.1f} in)")
            print(f"    Median: {precip.median():.1f} mm ({precip.median() / 25.4:.1f} in)")
            print(f"    10th %%: {precip.quantile(0.10):.1f} mm")
            print(f"    90th %%: {precip.quantile(0.90):.1f} mm")

    # Air temp patterns
    if "temp_mean_7d" in weather_df.columns:
        temp = weather_df["temp_mean_7d"].dropna()
        if len(temp) > 50:
            print(f"\n  AIR TEMPERATURE in 7 days before fruiting:")
            print(f"    Mean:   {temp.mean():.1f}°C ({temp.mean() * 9/5 + 32:.1f}°F)")
            print(f"    Median: {temp.median():.1f}°C ({temp.median() * 9/5 + 32:.1f}°F)")
            print(f"    10th %%: {temp.quantile(0.10):.1f}°C ({temp.quantile(0.10) * 9/5 + 32:.1f}°F)")
            print(f"    90th %%: {temp.quantile(0.90):.1f}°C ({temp.quantile(0.90) * 9/5 + 32:.1f}°F)")

    # Weather conditions by species
    if "soil_temp_mean_7d" in weather_df.columns:
        print(f"\n  Soil temp preferences by species (7-day mean):")
        for sp in weather_df["taxon_name"].value_counts().head(8).index:
            sp_soil = weather_df[weather_df["taxon_name"] == sp]["soil_temp_mean_7d"].dropna()
            if len(sp_soil) > 10:
                print(f"    {sp}: {sp_soil.median():.1f}°C / {sp_soil.median()*9/5+32:.0f}°F (n={len(sp_soil)})")

    # Scatter: soil temp vs precipitation
    if "soil_temp_mean_7d" in weather_df.columns and "precip_total_14d" in weather_df.columns:
        fig, ax = plt.subplots(figsize=(10, 6))
        plot_df = weather_df.dropna(subset=["soil_temp_mean_7d", "precip_total_14d"])
        scatter = ax.scatter(plot_df["soil_temp_mean_7d"], plot_df["precip_total_14d"],
                            s=10, alpha=0.4, c=plot_df["latitude"], cmap="RdYlBu_r")
        plt.colorbar(scatter, label="Latitude")
        ax.set_xlabel("Soil Temperature 0-7cm, 7-day mean (°C)")
        ax.set_ylabel("Precipitation, 14-day total (mm)")
        ax.set_title("Soil Temp vs. Precipitation at Morel Sightings")
        plt.tight_layout()
        plt.savefig(OUT / "soil_temp_vs_precip.png", dpi=150)
        plt.close()


def analyze_biomes(df):
    section("BIOME / ECOREGION ANALYSIS")

    biome_counts = df["biome"].dropna().value_counts()
    print(f"\n  Sightings by biome:")
    for biome, count in biome_counts.items():
        pct = count / len(df) * 100
        print(f"    {biome}: {count:,} ({pct:.1f}%)")

    eco_counts = df["ecoregion"].dropna().value_counts()
    print(f"\n  Top 20 ecoregions:")
    for eco, count in eco_counts.head(20).items():
        print(f"    {eco}: {count:,}")

    # Biome by species
    print(f"\n  Species distribution across biomes:")
    top_species = df["taxon_name"].value_counts().head(6).index
    biome_species = pd.crosstab(df["biome"], df["taxon_name"])
    biome_species = biome_species[[c for c in top_species if c in biome_species.columns]]
    # Normalize by row
    biome_species_pct = biome_species.div(biome_species.sum(axis=1), axis=0) * 100
    for biome in biome_species_pct.index[:8]:
        row = biome_species_pct.loc[biome]
        top = row.nlargest(3)
        specs = ", ".join(f"{sp} ({pct:.0f}%)" for sp, pct in top.items())
        print(f"    {biome}: {specs}")

    if len(biome_counts) > 1:
        fig, ax = plt.subplots(figsize=(12, 6))
        biome_counts.head(10).plot(kind="barh", ax=ax, color=sns.color_palette("YlGn_r", 10))
        ax.set_xlabel("Number of Sightings")
        ax.set_title("Morel Sightings by Biome")
        ax.invert_yaxis()
        plt.tight_layout()
        plt.savefig(OUT / "biome_distribution.png", dpi=150)
        plt.close()


def analyze_soil(df):
    section("SOIL COMPOSITION ANALYSIS")

    soil_cols = [c for c in df.columns if c.startswith("soil_") and c not in
                 ["soil_temp_mean_30d", "soil_temp_mean_7d", "soil_moisture_mean_30d", "soil_moisture_mean_7d"]]

    for col in soil_cols:
        vals = df[col].dropna()
        if len(vals) > 20:
            unit = "%" if "pct" in col else "g/kg" if "gkg" in col else ""
            print(f"\n  {col}:")
            print(f"    Mean: {vals.mean():.1f}{unit}  Median: {vals.median():.1f}{unit}")
            print(f"    Std: {vals.std():.1f}  Range: {vals.min():.1f} - {vals.max():.1f}")

    # Plot soil composition triangle if we have all three
    has_soil = df.dropna(subset=["soil_clay_pct", "soil_sand_pct", "soil_silt_pct"])
    if len(has_soil) > 20:
        fig, axes = plt.subplots(1, 3, figsize=(15, 4))
        for ax, col, label in zip(axes,
            ["soil_clay_pct", "soil_sand_pct", "soil_ph"],
            ["Clay %", "Sand %", "pH"]):
            vals = has_soil[col].dropna()
            if len(vals) > 10:
                ax.hist(vals, bins=30, color="sienna", alpha=0.7, edgecolor="white")
                ax.axvline(vals.median(), color="red", linestyle="--",
                           label=f"Median: {vals.median():.1f}")
                ax.set_xlabel(label)
                ax.set_ylabel("Count")
                ax.legend()
        axes[0].set_title("Clay Content")
        axes[1].set_title("Sand Content")
        axes[2].set_title("Soil pH")
        plt.suptitle("Soil Properties at Morel Sighting Locations", y=1.02)
        plt.tight_layout()
        plt.savefig(OUT / "soil_composition.png", dpi=150)
        plt.close()


def analyze_cospecies(cospecies_cache):
    section("CO-OCCURRING SPECIES ANALYSIS")

    # Aggregate species that appear near morel sightings
    all_species = Counter()
    iconic_taxa = Counter()
    plant_species = Counter()
    fungus_species = Counter()

    for obs_id, species_list in cospecies_cache.items():
        for sp in species_list:
            name = sp.get("name", "unknown")
            common = sp.get("common_name", "")
            iconic = sp.get("iconic_taxon", "")
            count = sp.get("count", 1)

            # Skip morels themselves
            if "Morchella" in name or "Verpa" in name:
                continue

            display = f"{name} ({common})" if common else name
            all_species[display] += count
            iconic_taxa[iconic] += count

            if iconic == "Plantae":
                plant_species[display] += count
            elif iconic == "Fungi":
                fungus_species[display] += count

    print(f"\n  Analyzed {len(cospecies_cache)} morel sighting locations")
    print(f"  Total co-occurring species found: {len(all_species)}")

    print(f"\n  By kingdom/group:")
    for taxon, count in iconic_taxa.most_common(10):
        print(f"    {taxon}: {count:,}")

    print(f"\n  Top 20 co-occurring PLANT species (within 5km, ±30 days):")
    for sp, count in plant_species.most_common(20):
        print(f"    {sp}: {count:,}")

    print(f"\n  Top 20 co-occurring FUNGUS species:")
    for sp, count in fungus_species.most_common(20):
        print(f"    {sp}: {count:,}")

    print(f"\n  Top 30 co-occurring species overall:")
    for sp, count in all_species.most_common(30):
        print(f"    {sp}: {count:,}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("Loading morel sighting data...")
    df = pd.read_csv("data/morel_sightings.csv", parse_dates=["observed_on"])
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude", "observed_on"])
    df["year"] = df["observed_on"].dt.year
    df["month"] = df["observed_on"].dt.month
    df["day_of_year"] = df["observed_on"].dt.dayofyear
    print(f"Loaded {len(df):,} observations with coordinates\n")

    # --- Step 1: Elevation (all observations) ---
    print("STEP 1: Enriching with elevation data...")
    df = enrich_elevation(df)
    analyze_elevation(df)

    # --- Step 2: Ecoregion (all observations, grid-cached) ---
    print("\nSTEP 2: Enriching with ecoregion/biome data...")
    df = enrich_ecoregion(df)
    analyze_biomes(df)

    # --- Step 3: Weather (sampled) ---
    print("\nSTEP 3: Enriching with weather data (sampled)...")
    weather_df = enrich_weather(df)
    analyze_weather(weather_df)

    # --- Step 4: Soil (sampled, slow) ---
    print("\nSTEP 4: Enriching with soil composition data...")
    df = enrich_soil(df)
    analyze_soil(df)

    # --- Step 5: Co-occurring species ---
    print("\nSTEP 5: Analyzing co-occurring species...")
    cospecies_cache = analyze_co_occurring_species(df)
    analyze_cospecies(cospecies_cache)

    # Save enriched dataset
    enriched_file = DATA / "morel_sightings_enriched.csv"
    df.to_csv(enriched_file, index=False)
    print(f"\nSaved enriched dataset to {enriched_file}")

    section("CHARTS SAVED")
    for f in sorted(OUT.glob("*.png")):
        print(f"  {f}")

    print("\nDone!")


if __name__ == "__main__":
    main()
