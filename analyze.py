"""
Exploratory analysis of morel mushroom sightings from iNaturalist.
Focused on findings relevant to the Framingham, MA area.
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from pathlib import Path
from math import radians, cos, sin, asin, sqrt

sns.set_theme(style="whitegrid")
OUT = Path("analysis")
OUT.mkdir(exist_ok=True)

# Framingham, MA coordinates
FRAMINGHAM_LAT = 42.2793
FRAMINGHAM_LON = -71.4162

def haversine(lat1, lon1, lat2, lon2):
    """Distance in miles between two lat/lon points."""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * 3956 * asin(sqrt(a))


def load_data():
    df = pd.read_csv("data/morel_sightings.csv", parse_dates=["observed_on"])
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude", "observed_on"])
    df["year"] = df["observed_on"].dt.year
    df["month"] = df["observed_on"].dt.month
    df["day_of_year"] = df["observed_on"].dt.dayofyear
    df["month_name"] = df["observed_on"].dt.strftime("%b")
    df["dist_from_framingham"] = df.apply(
        lambda r: haversine(FRAMINGHAM_LAT, FRAMINGHAM_LON, r["latitude"], r["longitude"]), axis=1
    )
    return df


def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def overall_stats(df):
    section("OVERALL DATASET STATS")
    print(f"Total observations: {len(df):,}")
    print(f"Date range: {df['observed_on'].min().date()} to {df['observed_on'].max().date()}")
    print(f"Unique observers: {df['user_login'].nunique():,}")
    print(f"Unique species/taxa: {df['taxon_name'].nunique()}")

    print(f"\nQuality grade breakdown:")
    for qg, count in df["quality_grade"].value_counts().items():
        print(f"  {qg}: {count:,} ({count/len(df)*100:.1f}%)")

    print(f"\nGeographic extent:")
    print(f"  Latitude:  {df['latitude'].min():.2f} to {df['latitude'].max():.2f}")
    print(f"  Longitude: {df['longitude'].min():.2f} to {df['longitude'].max():.2f}")


def temporal_analysis(df):
    section("TEMPORAL ANALYSIS — When do morels fruit?")

    # Focus on Northern Hemisphere temperate zone for meaningful seasonality
    nh = df[(df["latitude"] > 25) & (df["latitude"] < 60)]

    print(f"\nNorthern Hemisphere temperate observations: {len(nh):,}")

    # Month distribution
    month_counts = nh["month"].value_counts().sort_index()
    print(f"\nSightings by month (N. Hemisphere temperate):")
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    for m, count in month_counts.items():
        bar = "█" * (count // 200)
        print(f"  {month_names[m-1]}: {count:>6,}  {bar}")

    # Day of year stats
    peak_months = nh[nh["month"].isin([3, 4, 5, 6])]
    print(f"\nPeak season (Mar-Jun) day-of-year stats:")
    print(f"  Mean:   day {peak_months['day_of_year'].mean():.0f} ({pd.Timestamp('2024-01-01') + pd.Timedelta(days=int(peak_months['day_of_year'].mean())-1):%b %d})")
    print(f"  Median: day {peak_months['day_of_year'].median():.0f} ({pd.Timestamp('2024-01-01') + pd.Timedelta(days=int(peak_months['day_of_year'].median())-1):%b %d})")
    mode_doy = peak_months['day_of_year'].mode().iloc[0]
    print(f"  Mode:   day {mode_doy} ({pd.Timestamp('2024-01-01') + pd.Timedelta(days=int(mode_doy)-1):%b %d})")
    print(f"  Std:    {peak_months['day_of_year'].std():.1f} days")
    p5 = peak_months['day_of_year'].quantile(0.05)
    p95 = peak_months['day_of_year'].quantile(0.95)
    print(f"  5th percentile:  day {p5:.0f} ({pd.Timestamp('2024-01-01') + pd.Timedelta(days=int(p5)-1):%b %d})")
    print(f"  95th percentile: day {p95:.0f} ({pd.Timestamp('2024-01-01') + pd.Timedelta(days=int(p95)-1):%b %d})")

    # Plot: sightings by month
    fig, ax = plt.subplots(figsize=(10, 5))
    month_counts.plot(kind="bar", ax=ax, color=sns.color_palette("YlOrBr", 12))
    ax.set_xticklabels(month_names, rotation=0)
    ax.set_xlabel("Month")
    ax.set_ylabel("Number of Sightings")
    ax.set_title("Morel Sightings by Month (N. Hemisphere Temperate)")
    plt.tight_layout()
    plt.savefig(OUT / "sightings_by_month.png", dpi=150)
    plt.close()

    # Plot: timing by latitude (heatmap)
    fig, ax = plt.subplots(figsize=(12, 6))
    lat_bins = pd.cut(nh["latitude"], bins=np.arange(25, 61, 2.5))
    doy_bins = pd.cut(nh["day_of_year"], bins=np.arange(0, 366, 10))
    heatmap_data = pd.crosstab(lat_bins, doy_bins)
    # Only show Mar-Jul columns (days 60-210)
    cols = [c for c in heatmap_data.columns if c.left >= 60 and c.right <= 210]
    heatmap_data = heatmap_data[cols]
    sns.heatmap(heatmap_data.iloc[::-1], cmap="YlOrBr", ax=ax, cbar_kws={"label": "Sightings"})
    ax.set_xlabel("Day of Year (10-day bins, Mar–Jul)")
    ax.set_ylabel("Latitude Band")
    ax.set_title("Morel Fruiting Timing by Latitude — The 'Wave' Moving North")
    plt.tight_layout()
    plt.savefig(OUT / "latitude_timing_heatmap.png", dpi=150)
    plt.close()

    return nh


def species_analysis(df):
    section("SPECIES BREAKDOWN")

    species = df["taxon_name"].value_counts()
    print(f"\nTop 20 taxa:")
    for i, (sp, count) in enumerate(species.head(20).items(), 1):
        common = df[df["taxon_name"] == sp]["taxon_common_name"].mode()
        common_str = f" ({common.iloc[0]})" if len(common) > 0 and pd.notna(common.iloc[0]) else ""
        print(f"  {i:>2}. {sp}{common_str}: {count:,}")

    # Research-grade species only
    rg = df[df["quality_grade"] == "research"]
    rg_species = rg["taxon_name"].value_counts()
    print(f"\nTop 10 research-grade species:")
    for sp, count in rg_species.head(10).items():
        print(f"  {sp}: {count:,}")

    # Plot species distribution
    fig, ax = plt.subplots(figsize=(10, 6))
    top10 = species.head(10)
    top10.plot(kind="barh", ax=ax, color=sns.color_palette("YlOrBr_r", 10))
    ax.set_xlabel("Number of Sightings")
    ax.set_title("Top 10 Morel Taxa by Sighting Count")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(OUT / "species_distribution.png", dpi=150)
    plt.close()


def geographic_analysis(df):
    section("GEOGRAPHIC HOTSPOTS")

    # Rough country/region assignment based on coordinates
    us = df[(df["latitude"] > 24) & (df["latitude"] < 50) &
            (df["longitude"] > -125) & (df["longitude"] < -66)]
    europe = df[(df["latitude"] > 35) & (df["latitude"] < 72) &
                (df["longitude"] > -10) & (df["longitude"] < 40)]

    print(f"\nApproximate regional breakdown:")
    print(f"  USA/Canada: {len(us):,}")
    print(f"  Europe: {len(europe):,}")
    print(f"  Other: {len(df) - len(us) - len(europe):,}")

    # US state-level hotspots using rough lat/lon bounding boxes
    # Use place_guess to extract states
    us_places = us["place_guess"].dropna()
    state_abbrevs = [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
        "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
        "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
        "TX","UT","VT","VA","WA","WV","WI","WY"
    ]
    state_names = {
        "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
        "Colorado":"CO","Connecticut":"CT","Delaware":"DE","Florida":"FL","Georgia":"GA",
        "Hawaii":"HI","Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA","Kansas":"KS",
        "Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD","Massachusetts":"MA",
        "Michigan":"MI","Minnesota":"MN","Mississippi":"MS","Missouri":"MO","Montana":"MT",
        "Nebraska":"NE","Nevada":"NV","New Hampshire":"NH","New Jersey":"NJ",
        "New Mexico":"NM","New York":"NY","North Carolina":"NC","North Dakota":"ND",
        "Ohio":"OH","Oklahoma":"OK","Oregon":"OR","Pennsylvania":"PA","Rhode Island":"RI",
        "South Carolina":"SC","South Dakota":"SD","Tennessee":"TN","Texas":"TX","Utah":"UT",
        "Vermont":"VT","Virginia":"VA","Washington":"WA","West Virginia":"WV",
        "Wisconsin":"WI","Wyoming":"WY"
    }

    def extract_state(place):
        if not isinstance(place, str):
            return None
        # Check for state abbreviations (2-letter codes often at end or after comma)
        for state, abbr in state_names.items():
            if state in place:
                return abbr
        # Check for abbreviations like ", MA" or ", CA, US"
        parts = place.split(",")
        for part in parts:
            stripped = part.strip().upper()
            if stripped in state_abbrevs:
                return stripped
            if len(stripped) >= 2 and stripped[:2] in state_abbrevs:
                return stripped[:2]
        return None

    us["state"] = us["place_guess"].apply(extract_state)
    state_counts = us["state"].dropna().value_counts()

    print(f"\nTop 15 US states for morel sightings:")
    for state, count in state_counts.head(15).items():
        bar = "█" * (count // 100)
        print(f"  {state}: {count:>5,}  {bar}")

    # Plot: scatter map of all sightings
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.scatter(df["longitude"], df["latitude"], s=0.5, alpha=0.3, c="sienna")
    ax.scatter(FRAMINGHAM_LON, FRAMINGHAM_LAT, s=200, c="red", marker="*",
               zorder=5, label="Framingham, MA")
    ax.set_xlim(-130, 50)
    ax.set_ylim(20, 70)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("Global Morel Sightings (iNaturalist)")
    ax.legend(fontsize=12)
    plt.tight_layout()
    plt.savefig(OUT / "global_sightings_map.png", dpi=150)
    plt.close()

    return us


def new_england_analysis(df):
    section("NEW ENGLAND / FRAMINGHAM AREA ANALYSIS")

    # New England bounding box
    ne = df[(df["latitude"] > 40.5) & (df["latitude"] < 47.5) &
            (df["longitude"] > -73.8) & (df["longitude"] < -66.9)]

    # Massachusetts specifically
    ma = df[df["place_guess"].str.contains("Massachusetts|, MA", case=False, na=False)]

    print(f"\nNew England observations: {len(ne):,}")
    print(f"Massachusetts observations: {len(ma):,}")

    if len(ma) > 0:
        print(f"\nMassachusetts morel stats:")
        print(f"  Date range: {ma['observed_on'].min().date()} to {ma['observed_on'].max().date()}")
        print(f"  Unique observers: {ma['user_login'].nunique()}")

        ma_species = ma["taxon_name"].value_counts()
        print(f"\n  Species found in MA:")
        for sp, count in ma_species.items():
            print(f"    {sp}: {count}")

        print(f"\n  Timing in MA (day of year):")
        print(f"    Earliest: {ma['observed_on'].min().date()} (day {ma['day_of_year'].min()})")
        print(f"    Latest:   {ma['observed_on'].max().date()} (day {ma['day_of_year'].max()})")
        print(f"    Mean:     day {ma['day_of_year'].mean():.0f} ({pd.Timestamp('2024-01-01') + pd.Timedelta(days=int(ma['day_of_year'].mean())-1):%b %d})")
        print(f"    Median:   day {ma['day_of_year'].median():.0f} ({pd.Timestamp('2024-01-01') + pd.Timedelta(days=int(ma['day_of_year'].median())-1):%b %d})")

        ma_month = ma["month"].value_counts().sort_index()
        print(f"\n  MA sightings by month:")
        month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        for m, count in ma_month.items():
            print(f"    {month_names[m-1]}: {count}")

    # Closest sightings to Framingham
    nearby = df[df["dist_from_framingham"] < 50].copy()
    nearby = nearby.sort_values("dist_from_framingham")

    print(f"\n  Sightings within 50 miles of Framingham: {len(nearby):,}")
    if len(nearby) > 0:
        print(f"\n  10 closest sightings to Framingham:")
        for _, row in nearby.head(10).iterrows():
            print(f"    {row['dist_from_framingham']:.1f} mi — {row['observed_on'].date()} — "
                  f"{row['taxon_name']} — {row['place_guess']}")

        print(f"\n  Nearby timing stats:")
        print(f"    Earliest: {nearby['observed_on'].min().date()}")
        print(f"    Latest:   {nearby['observed_on'].max().date()}")
        print(f"    Mean day: {nearby['day_of_year'].mean():.0f} ({pd.Timestamp('2024-01-01') + pd.Timedelta(days=int(nearby['day_of_year'].mean())-1):%b %d})")
        print(f"    Median:   {nearby['day_of_year'].median():.0f} ({pd.Timestamp('2024-01-01') + pd.Timedelta(days=int(nearby['day_of_year'].median())-1):%b %d})")

        nearby_species = nearby["taxon_name"].value_counts()
        print(f"\n  Species within 50mi of Framingham:")
        for sp, count in nearby_species.items():
            print(f"    {sp}: {count}")

    # Plot: New England map
    if len(ne) > 0:
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.scatter(ne["longitude"], ne["latitude"], s=10, alpha=0.5, c="sienna", label="Morel sighting")
        ax.scatter(FRAMINGHAM_LON, FRAMINGHAM_LAT, s=200, c="red", marker="*",
                   zorder=5, label="Framingham, MA")
        # Draw 50-mile radius circle (approx)
        theta = np.linspace(0, 2*np.pi, 100)
        r_deg_lat = 50 / 69.0  # rough miles to degrees
        r_deg_lon = 50 / (69.0 * cos(radians(FRAMINGHAM_LAT)))
        ax.plot(FRAMINGHAM_LON + r_deg_lon * np.cos(theta),
                FRAMINGHAM_LAT + r_deg_lat * np.sin(theta),
                'r--', alpha=0.5, label="50-mile radius")
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.set_title("New England Morel Sightings")
        ax.legend()
        plt.tight_layout()
        plt.savefig(OUT / "new_england_map.png", dpi=150)
        plt.close()


def outliers_and_oddities(df):
    section("OUTLIERS & INTERESTING FINDINGS")

    # Extreme latitudes
    print(f"\nMost northern sighting:")
    north = df.loc[df["latitude"].idxmax()]
    print(f"  {north['latitude']:.2f}°N — {north['place_guess']} — {north['observed_on'].date()} — {north['taxon_name']}")

    print(f"\nMost southern sighting:")
    south = df.loc[df["latitude"].idxmin()]
    print(f"  {south['latitude']:.2f}°N — {south['place_guess']} — {south['observed_on'].date()} — {south['taxon_name']}")

    # Off-season sightings (Dec-Feb in Northern Hemisphere)
    nh = df[(df["latitude"] > 30)]
    winter = nh[nh["month"].isin([12, 1, 2])]
    print(f"\nWinter sightings (Dec-Feb) in Northern Hemisphere (>30°N): {len(winter):,}")
    if len(winter) > 0:
        winter_species = winter["taxon_name"].value_counts().head(5)
        print(f"  Top winter species:")
        for sp, count in winter_species.items():
            print(f"    {sp}: {count}")

    # Most prolific observers
    print(f"\nTop 10 most prolific morel observers:")
    top_observers = df["user_login"].value_counts().head(10)
    for user, count in top_observers.items():
        print(f"  {user}: {count:,} observations")

    # Observations per year growth
    print(f"\nYear-over-year growth:")
    yearly = df[df["year"] >= 2015].groupby("year").size()
    for i in range(1, len(yearly)):
        yr = yearly.index[i]
        prev = yearly.iloc[i-1]
        curr = yearly.iloc[i]
        if prev > 0:
            pct = (curr - prev) / prev * 100
            print(f"  {yr}: {curr:,} ({pct:+.1f}% vs prior year)")

    # Elevation proxy: latitude vs timing correlation
    nh_spring = df[(df["latitude"] > 30) & (df["latitude"] < 50) & (df["month"].isin([3,4,5,6]))]
    if len(nh_spring) > 100:
        corr = nh_spring["latitude"].corr(nh_spring["day_of_year"])
        print(f"\nLatitude vs. day-of-year correlation (spring, N. Hemisphere): r={corr:.3f}")
        print(f"  → {'Strong' if abs(corr) > 0.4 else 'Moderate' if abs(corr) > 0.2 else 'Weak'} positive correlation: morels fruit later at higher latitudes")

        # Estimate timing per degree of latitude
        from numpy.polynomial import polynomial as P
        mask = nh_spring[["latitude", "day_of_year"]].dropna()
        coeffs = np.polyfit(mask["latitude"], mask["day_of_year"], 1)
        print(f"  Linear fit: ~{coeffs[0]:.1f} days later per degree of latitude north")
        print(f"  At Framingham's latitude ({FRAMINGHAM_LAT:.1f}°N), predicted peak: "
              f"day {int(coeffs[0]*FRAMINGHAM_LAT + coeffs[1])} "
              f"({pd.Timestamp('2024-01-01') + pd.Timedelta(days=int(coeffs[0]*FRAMINGHAM_LAT + coeffs[1])-1):%b %d})")


def yearly_progression(df):
    section("SEASONAL PROGRESSION — How the 'Morel Wave' Moves North")

    # For each year, show the median latitude by week during spring
    spring = df[(df["month"].isin([3,4,5,6])) & (df["latitude"] > 25) & (df["latitude"] < 50)]
    spring = spring[spring["year"] >= 2020]
    spring["week"] = spring["observed_on"].dt.isocalendar().week.astype(int)

    weekly_lat = spring.groupby("week").agg(
        median_lat=("latitude", "median"),
        mean_lat=("latitude", "mean"),
        count=("latitude", "size")
    )
    weekly_lat = weekly_lat[weekly_lat["count"] >= 10]

    print(f"\nMedian latitude of sightings by week (2020+, Mar-Jun):")
    print(f"  {'Week':>4}  {'~Date':>8}  {'Med Lat':>7}  {'Count':>5}")
    for week, row in weekly_lat.iterrows():
        approx_date = pd.Timestamp("2024-01-01") + pd.Timedelta(weeks=int(week)-1)
        print(f"  {week:>4}  {approx_date:%b %d}  {row['median_lat']:>7.1f}°  {int(row['count']):>5}")

    # Plot the wave
    fig, ax = plt.subplots(figsize=(12, 6))
    for year in sorted(spring["year"].unique()):
        yr_data = spring[spring["year"] == year].groupby("week")["latitude"].median()
        if len(yr_data) > 3:
            ax.plot(yr_data.index, yr_data.values, alpha=0.3, linewidth=1, color="sienna")
    # Overall trend
    overall = spring.groupby("week")["latitude"].median()
    ax.plot(overall.index, overall.values, linewidth=3, color="darkred", label="Overall median")
    ax.axhline(y=FRAMINGHAM_LAT, color="blue", linestyle="--", alpha=0.7, label=f"Framingham ({FRAMINGHAM_LAT}°N)")
    ax.set_xlabel("Week of Year")
    ax.set_ylabel("Median Latitude of Sightings")
    ax.set_title("The Morel Wave: How Fruiting Moves North Through Spring")
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT / "morel_wave.png", dpi=150)
    plt.close()


def main():
    print("Loading data...")
    df = load_data()

    overall_stats(df)
    nh = temporal_analysis(df)
    species_analysis(df)
    us = geographic_analysis(df)
    new_england_analysis(df)
    outliers_and_oddities(df)
    yearly_progression(df)

    section("CHARTS SAVED")
    for f in sorted(OUT.glob("*.png")):
        print(f"  {f}")

    print("\nDone!")


if __name__ == "__main__":
    main()
