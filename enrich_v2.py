"""
Enrich morel sightings with environmental data and run correlation analysis.
V2: Skip elevation API (rate-limited), focus on weather, ecoregion, co-species.
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

FRAMINGHAM_LAT = 42.2793
FRAMINGHAM_LON = -71.4162


def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def load_data():
    df = pd.read_csv("data/morel_sightings.csv", parse_dates=["observed_on"])
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude", "observed_on"])
    df["year"] = df["observed_on"].dt.year
    df["month"] = df["observed_on"].dt.month
    df["day_of_year"] = df["observed_on"].dt.dayofyear
    return df


# ---------------------------------------------------------------------------
# WEATHER (Open-Meteo Historical API)
# ---------------------------------------------------------------------------
def fetch_weather(lat, lon, end_date, lookback_days=30):
    start = (pd.Timestamp(end_date) - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start, "end_date": end,
        "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,rain_sum",
        "hourly": "soil_temperature_0_to_7cm,soil_moisture_0_to_7cm",
        "timezone": "auto",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    daily = data.get("daily", {})
    hourly = data.get("hourly", {})

    def safe_mean(vals):
        clean = [v for v in (vals or []) if v is not None]
        return np.mean(clean) if clean else None

    def safe_sum(vals):
        clean = [v for v in (vals or []) if v is not None]
        return np.sum(clean) if clean else None

    def last_n(vals, n):
        return [v for v in (vals or [])[-n:] if v is not None]

    return {
        "temp_mean_30d": safe_mean(daily.get("temperature_2m_mean")),
        "temp_max_30d": safe_mean(daily.get("temperature_2m_max")),
        "temp_min_30d": safe_mean(daily.get("temperature_2m_min")),
        "precip_total_30d_mm": safe_sum(daily.get("precipitation_sum")),
        "precip_total_7d_mm": safe_sum(last_n(daily.get("precipitation_sum"), 7)),
        "precip_total_14d_mm": safe_sum(last_n(daily.get("precipitation_sum"), 14)),
        "temp_mean_7d": safe_mean(last_n(daily.get("temperature_2m_mean"), 7)),
        "temp_mean_14d": safe_mean(last_n(daily.get("temperature_2m_mean"), 14)),
        "soil_temp_mean_30d": safe_mean(hourly.get("soil_temperature_0_to_7cm")),
        "soil_temp_mean_7d": safe_mean(
            (hourly.get("soil_temperature_0_to_7cm") or [])[-168:]
        ),
        "soil_moisture_mean_30d": safe_mean(hourly.get("soil_moisture_0_to_7cm")),
        "soil_moisture_mean_7d": safe_mean(
            (hourly.get("soil_moisture_0_to_7cm") or [])[-168:]
        ),
    }


def enrich_weather(df, sample_size=1000):
    cache_file = DATA / "weather_cache.json"
    cache = {}
    if cache_file.exists():
        with open(cache_file) as f:
            cache = json.load(f)
        print(f"  Loaded {len(cache)} cached weather records")

    # Sample research-grade, 2010+
    rg = df[(df["quality_grade"] == "research") & (df["year"] >= 2010)].copy()
    samples = []
    for year, group in rg.groupby("year"):
        n = min(len(group), max(30, int(sample_size * len(group) / len(rg))))
        samples.append(group.sample(n=n, random_state=42))
    sample = pd.concat(samples)

    need = sample[~sample["id"].astype(str).isin(cache)]
    print(f"  Weather sample: {len(sample):,} obs, {len(need):,} need fetching")

    errors = 0
    for i, (_, row) in enumerate(need.iterrows()):
        obs_id = str(row["id"])
        try:
            weather = fetch_weather(row["latitude"], row["longitude"], row["observed_on"])
            cache[obs_id] = weather
            errors = 0
        except Exception as e:
            errors += 1
            if errors >= 10:
                print(f"\n  Too many errors, stopping. Last: {e}")
                break
            time.sleep(5)
            continue

        if (i + 1) % 20 == 0:
            print(f"\r  Weather: {i+1:,}/{len(need):,}", end="", flush=True)
        time.sleep(0.3)

        if (i + 1) % 200 == 0:
            with open(cache_file, "w") as f:
                json.dump(cache, f)

    print()
    with open(cache_file, "w") as f:
        json.dump(cache, f)

    weather_df = pd.DataFrame([{"id": int(k), **v} for k, v in cache.items()])
    sample = sample.merge(weather_df, on="id", how="left")
    return sample


# ---------------------------------------------------------------------------
# ECOREGION / BIOME (UNEP-WCMC ArcGIS)
# ---------------------------------------------------------------------------
def fetch_ecoregion(lat, lon):
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
    features = resp.json().get("features", [])
    if features:
        attrs = features[0].get("attributes", {})
        return {
            "ecoregion": attrs.get("eco_name"),
            "biome": attrs.get("biome_name"),
            "realm": attrs.get("realm"),
        }
    return {"ecoregion": None, "biome": None, "realm": None}


def enrich_ecoregion(df):
    cache_file = DATA / "ecoregion_cache.json"
    cache = {}
    if cache_file.exists():
        with open(cache_file) as f:
            cache = json.load(f)
        print(f"  Loaded {len(cache)} cached ecoregion lookups")

    # 1 degree grid (ecoregions are large, this is fine)
    df["eco_key"] = (
        df["latitude"].round()
    ).astype(str) + "," + (
        df["longitude"].round()
    ).astype(str)

    unique_keys = df["eco_key"].unique()
    need = [k for k in unique_keys if k not in cache]
    print(f"  Ecoregion: {len(unique_keys)} grid cells, {len(need)} need fetching")

    errors = 0
    for i, key in enumerate(need):
        lat, lon = key.split(",")
        try:
            result = fetch_ecoregion(float(lat), float(lon))
            cache[key] = result
            errors = 0
        except Exception as e:
            errors += 1
            cache[key] = {"ecoregion": None, "biome": None, "realm": None}
            if errors >= 10:
                print(f"\n  Too many errors, stopping. Last: {e}")
                break
            time.sleep(3)
            continue

        if (i + 1) % 20 == 0:
            print(f"\r  Ecoregion: {i+1:,}/{len(need):,}", end="", flush=True)
        time.sleep(0.15)

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
# CO-OCCURRING SPECIES (iNaturalist)
# ---------------------------------------------------------------------------
def fetch_nearby_species(lat, lon, date, radius_km=5, days_window=30):
    start = (pd.Timestamp(date) - pd.Timedelta(days=days_window)).strftime("%Y-%m-%d")
    end = pd.Timestamp(date).strftime("%Y-%m-%d")
    url = "https://api.inaturalist.org/v1/observations/species_counts"
    params = {
        "lat": lat, "lng": lon, "radius": radius_km,
        "d1": start, "d2": end,
        "quality_grade": "research", "per_page": 50,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return [
        {
            "name": r.get("taxon", {}).get("name"),
            "common_name": r.get("taxon", {}).get("preferred_common_name"),
            "iconic_taxon": r.get("taxon", {}).get("iconic_taxon_name"),
            "count": r.get("count"),
        }
        for r in resp.json().get("results", [])
    ]


def enrich_co_species(df, n_samples=150):
    cache_file = DATA / "cospecies_cache.json"
    cache = {}
    if cache_file.exists():
        with open(cache_file) as f:
            cache = json.load(f)
        print(f"  Loaded {len(cache)} cached co-species lookups")

    rg = df[(df["quality_grade"] == "research") & (df["year"] >= 2018)]
    sample = rg.sample(n=min(n_samples, len(rg)), random_state=42)
    need = sample[~sample["id"].astype(str).isin(cache)]
    print(f"  Co-species: {len(sample)} sampled, {len(need)} need fetching")

    for i, (_, row) in enumerate(need.iterrows()):
        obs_id = str(row["id"])
        try:
            species = fetch_nearby_species(row["latitude"], row["longitude"], row["observed_on"])
            cache[obs_id] = species
        except Exception as e:
            print(f"\n  Error: {e}")
            time.sleep(5)
            continue

        if (i + 1) % 10 == 0:
            print(f"\r  Co-species: {i+1:,}/{len(need):,}", end="", flush=True)
        time.sleep(1.0)

        if (i + 1) % 50 == 0:
            with open(cache_file, "w") as f:
                json.dump(cache, f)

    print()
    with open(cache_file, "w") as f:
        json.dump(cache, f)
    return cache


# ---------------------------------------------------------------------------
# ANALYSIS
# ---------------------------------------------------------------------------
def analyze_weather(weather_df):
    section("WEATHER CORRELATION ANALYSIS")

    weather_cols = [c for c in weather_df.columns if c.startswith(("temp_", "precip_", "soil_t", "soil_m"))]
    print(f"  Weather-enriched observations: {len(weather_df):,}")

    print(f"\n  Conditions at time of morel sighting:")
    for col in weather_cols:
        vals = weather_df[col].dropna()
        if len(vals) > 10:
            unit = "°C" if "temp" in col else "mm" if "precip" in col else "m³/m³"
            print(f"    {col}: mean={vals.mean():.1f} median={vals.median():.1f} std={vals.std():.1f} {unit}")

    # Soil temperature — the magic number
    if "soil_temp_mean_7d" in weather_df.columns:
        soil = weather_df["soil_temp_mean_7d"].dropna()
        if len(soil) > 50:
            print(f"\n  === SOIL TEMPERATURE SWEET SPOT (7-day mean, 0-7cm) ===")
            print(f"    Mean:    {soil.mean():.1f}°C / {soil.mean()*9/5+32:.0f}°F")
            print(f"    Median:  {soil.median():.1f}°C / {soil.median()*9/5+32:.0f}°F")
            print(f"    10th %%:  {soil.quantile(0.10):.1f}°C / {soil.quantile(0.10)*9/5+32:.0f}°F")
            print(f"    25th %%:  {soil.quantile(0.25):.1f}°C / {soil.quantile(0.25)*9/5+32:.0f}°F")
            print(f"    75th %%:  {soil.quantile(0.75):.1f}°C / {soil.quantile(0.75)*9/5+32:.0f}°F")
            print(f"    90th %%:  {soil.quantile(0.90):.1f}°C / {soil.quantile(0.90)*9/5+32:.0f}°F")

            fig, ax = plt.subplots(figsize=(10, 5))
            ax.hist(soil, bins=40, color="sienna", alpha=0.7, edgecolor="white")
            ax.axvline(soil.median(), color="red", linestyle="--",
                       label=f"Median: {soil.median():.1f}°C / {soil.median()*9/5+32:.0f}°F")
            ax.set_xlabel("Soil Temperature 0-7cm (°C)")
            ax.set_ylabel("Count")
            ax.set_title("Soil Temperature When Morels Were Found (7-day mean)")
            ax.legend(fontsize=12)
            plt.tight_layout()
            plt.savefig(OUT / "soil_temp_sweet_spot.png", dpi=150)
            plt.close()

    # Precipitation
    if "precip_total_14d_mm" in weather_df.columns:
        precip = weather_df["precip_total_14d_mm"].dropna()
        if len(precip) > 50:
            print(f"\n  === PRECIPITATION (14 days before fruiting) ===")
            print(f"    Mean:    {precip.mean():.1f} mm / {precip.mean()/25.4:.1f} in")
            print(f"    Median:  {precip.median():.1f} mm / {precip.median()/25.4:.1f} in")
            print(f"    10th %%:  {precip.quantile(0.10):.1f} mm")
            print(f"    90th %%:  {precip.quantile(0.90):.1f} mm")

    # Air temp
    if "temp_mean_7d" in weather_df.columns:
        temp = weather_df["temp_mean_7d"].dropna()
        if len(temp) > 50:
            print(f"\n  === AIR TEMPERATURE (7 days before fruiting) ===")
            print(f"    Mean:    {temp.mean():.1f}°C / {temp.mean()*9/5+32:.0f}°F")
            print(f"    Median:  {temp.median():.1f}°C / {temp.median()*9/5+32:.0f}°F")
            print(f"    10th %%:  {temp.quantile(0.10):.1f}°C / {temp.quantile(0.10)*9/5+32:.0f}°F")
            print(f"    90th %%:  {temp.quantile(0.90):.1f}°C / {temp.quantile(0.90)*9/5+32:.0f}°F")

    # Species-specific weather preferences
    if "soil_temp_mean_7d" in weather_df.columns:
        print(f"\n  Soil temp by species (7-day mean before sighting):")
        for sp in weather_df["taxon_name"].value_counts().head(8).index:
            sp_soil = weather_df[weather_df["taxon_name"] == sp]["soil_temp_mean_7d"].dropna()
            if len(sp_soil) > 10:
                print(f"    {sp}: median {sp_soil.median():.1f}°C / {sp_soil.median()*9/5+32:.0f}°F [n={len(sp_soil)}]")

    # Correlation matrix
    corr_cols = [c for c in weather_cols + ["day_of_year", "latitude"] if c in weather_df.columns]
    corr_data = weather_df[corr_cols].dropna()
    if len(corr_data) > 50:
        corr = corr_data.corr()
        fig, ax = plt.subplots(figsize=(14, 10))
        mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
        sns.heatmap(corr, mask=mask, annot=True, fmt=".2f", cmap="RdBu_r",
                    center=0, ax=ax, square=True, linewidths=0.5)
        ax.set_title("Correlation Matrix: Weather Variables at Morel Sightings")
        plt.tight_layout()
        plt.savefig(OUT / "weather_correlation_matrix.png", dpi=150)
        plt.close()

    # Soil temp vs precip scatter
    if "soil_temp_mean_7d" in weather_df.columns and "precip_total_14d_mm" in weather_df.columns:
        plot_df = weather_df.dropna(subset=["soil_temp_mean_7d", "precip_total_14d_mm"])
        if len(plot_df) > 50:
            fig, ax = plt.subplots(figsize=(10, 6))
            sc = ax.scatter(plot_df["soil_temp_mean_7d"], plot_df["precip_total_14d_mm"],
                           s=10, alpha=0.4, c=plot_df["latitude"], cmap="RdYlBu_r")
            plt.colorbar(sc, label="Latitude")
            ax.set_xlabel("Soil Temp 0-7cm, 7-day mean (°C)")
            ax.set_ylabel("Precipitation, 14-day total (mm)")
            ax.set_title("Soil Temp vs. Precipitation at Morel Sightings")
            # Add sweet spot box
            ax.axvspan(8, 16, alpha=0.1, color="green", label="Sweet spot")
            ax.legend()
            plt.tight_layout()
            plt.savefig(OUT / "soil_temp_vs_precip.png", dpi=150)
            plt.close()

    # Soil moisture
    if "soil_moisture_mean_7d" in weather_df.columns:
        moisture = weather_df["soil_moisture_mean_7d"].dropna()
        if len(moisture) > 50:
            print(f"\n  === SOIL MOISTURE (7 days before fruiting, 0-7cm) ===")
            print(f"    Mean:    {moisture.mean():.3f} m³/m³")
            print(f"    Median:  {moisture.median():.3f} m³/m³")
            print(f"    10th %%:  {moisture.quantile(0.10):.3f}")
            print(f"    90th %%:  {moisture.quantile(0.90):.3f}")


def analyze_biomes(df):
    section("BIOME / ECOREGION ANALYSIS")

    biome_counts = df["biome"].dropna().value_counts()
    total_with_biome = df["biome"].notna().sum()
    print(f"\n  Observations with biome data: {total_with_biome:,}")
    print(f"\n  Sightings by biome:")
    for biome, count in biome_counts.head(12).items():
        pct = count / total_with_biome * 100
        bar = "█" * int(pct / 2)
        print(f"    {biome}: {count:,} ({pct:.1f}%) {bar}")

    eco_counts = df["ecoregion"].dropna().value_counts()
    print(f"\n  Top 20 ecoregions:")
    for eco, count in eco_counts.head(20).items():
        print(f"    {eco}: {count:,}")

    # Which species dominate each biome?
    print(f"\n  Dominant species per biome:")
    top_biomes = biome_counts.head(6).index
    for biome in top_biomes:
        biome_df = df[df["biome"] == biome]
        top_sp = biome_df["taxon_name"].value_counts().head(3)
        specs = ", ".join(f"{sp} ({c})" for sp, c in top_sp.items())
        print(f"    {biome}:")
        print(f"      {specs}")

    if len(biome_counts) > 1:
        fig, ax = plt.subplots(figsize=(12, 6))
        biome_counts.head(10).plot(kind="barh", ax=ax, color=sns.color_palette("YlGn_r", 10))
        ax.set_xlabel("Number of Sightings")
        ax.set_title("Morel Sightings by Biome")
        ax.invert_yaxis()
        plt.tight_layout()
        plt.savefig(OUT / "biome_distribution.png", dpi=150)
        plt.close()


def analyze_cospecies(cache):
    section("CO-OCCURRING SPECIES ANALYSIS")

    plant_species = Counter()
    fungus_species = Counter()
    tree_species = Counter()
    all_species = Counter()
    iconic_taxa = Counter()

    for obs_id, species_list in cache.items():
        for sp in species_list:
            name = sp.get("name", "unknown")
            common = sp.get("common_name", "")
            iconic = sp.get("iconic_taxon", "")
            count = sp.get("count", 1)
            if "Morchella" in name or "Verpa" in name:
                continue

            display = f"{name} ({common})" if common else name
            all_species[display] += count
            iconic_taxa[iconic] += count
            if iconic == "Plantae":
                plant_species[display] += count
            elif iconic == "Fungi":
                fungus_species[display] += count

    print(f"  Analyzed {len(cache)} morel sighting locations")
    print(f"  Unique co-occurring species: {len(all_species)}")

    print(f"\n  By kingdom/group:")
    for taxon, count in iconic_taxa.most_common(10):
        print(f"    {taxon}: {count:,}")

    print(f"\n  Top 25 co-occurring PLANT species (within 5km, ±30 days):")
    for sp, count in plant_species.most_common(25):
        print(f"    {sp}: {count:,}")

    print(f"\n  Top 15 co-occurring FUNGI:")
    for sp, count in fungus_species.most_common(15):
        print(f"    {sp}: {count:,}")

    # Top overall
    print(f"\n  Top 25 co-occurring species (all kingdoms):")
    for sp, count in all_species.most_common(25):
        print(f"    {sp}: {count:,}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("Loading data...")
    df = load_data()
    print(f"Loaded {len(df):,} observations\n")

    # Step 1: Ecoregion
    print("STEP 1: Ecoregion/biome enrichment...")
    df = enrich_ecoregion(df)
    analyze_biomes(df)

    # Step 2: Weather
    print("\nSTEP 2: Weather enrichment (sampled)...")
    weather_df = enrich_weather(df)
    analyze_weather(weather_df)

    # Step 3: Co-occurring species
    print("\nSTEP 3: Co-occurring species...")
    cospecies = enrich_co_species(df)
    analyze_cospecies(cospecies)

    # Save enriched data
    df.to_csv(DATA / "morel_sightings_enriched.csv", index=False)
    print(f"\nSaved enriched dataset")

    section("CHARTS SAVED")
    for f in sorted(OUT.glob("*.png")):
        print(f"  {f}")
    print("\nDone!")


if __name__ == "__main__":
    main()
