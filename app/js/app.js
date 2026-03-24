/**
 * Morel Conditions — A foraging companion app
 *
 * Helps people find morels responsibly by showing when conditions
 * are right, where to look, and how to harvest with gratitude.
 */

// ── State ──────────────────────────────────────────────────────
let map;
let appConfig = {};
let densityGrid = [];
let sightingsData = null;
let harvestData = {};
let indicatorTaxa = [];

// Layers
let probabilityLayer = null;
let sightingsLayer = null;
let indicatorLayer = null;
let publicLandLayer = null;
let forestLayer = null;

// User location (updated by geolocation)
let userLocation = null;

// Weather cache: "lat,lon" -> { soilTemp, precip14d, fetchedAt }
const weatherCache = new Map();
const CACHE_TTL = 3 * 60 * 60 * 1000; // 3 hours

// ── Init ───────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
  try {
    await loadData();
    initMap();
    initUI();
    geolocateUser();
    await fetchWeatherAndRender();
    loadSightingsLayer();
    loadIndicatorLayer();
  } catch (err) {
    console.error("Init error:", err);
    document.getElementById("readiness-label").textContent = "Error loading data";
  }
});

// ── Geolocation ────────────────────────────────────────────────
function geolocateUser() {
  // Geolocation requires HTTPS on mobile browsers.
  // On plain HTTP (dev), we fall back to the default center.
  const isSecure = location.protocol === "https:" || location.hostname === "localhost";

  if (!isSecure || !navigator.geolocation) {
    console.log("Geolocation unavailable (requires HTTPS). Using default center.");
    userLocation = { lat: appConfig.default_center[0], lon: appConfig.default_center[1] };
    addUserMarker(userLocation.lat, userLocation.lon);
    return;
  }

  document.getElementById("readiness-label").textContent = "Finding your location...";

  navigator.geolocation.getCurrentPosition(
    (pos) => {
      const { latitude, longitude } = pos.coords;
      userLocation = { lat: latitude, lon: longitude };
      map.setView([latitude, longitude], 10);
      addUserMarker(latitude, longitude);
    },
    (err) => {
      console.log("Geolocation denied, using default center");
      userLocation = { lat: appConfig.default_center[0], lon: appConfig.default_center[1] };
      addUserMarker(userLocation.lat, userLocation.lon);
    },
    { enableHighAccuracy: false, timeout: 8000, maximumAge: 300000 }
  );
}

function addUserMarker(lat, lon) {
  const userIcon = L.divIcon({
    className: "user-location-marker",
    html: '<div class="user-dot"><div class="user-dot-pulse"></div></div>',
    iconSize: [18, 18],
    iconAnchor: [9, 9],
  });
  L.marker([lat, lon], { icon: userIcon, zIndex: 9999 })
    .addTo(map)
    .bindPopup("Your area");
}

// ── Data Loading ───────────────────────────────────────────────
async function loadData() {
  const [config, density, sightings, harvest, indicators] = await Promise.all([
    fetch("data/app-config.json").then(r => r.json()),
    fetch("data/density-grid.json").then(r => r.json()),
    fetch("data/sightings-ma.geojson").then(r => r.json()),
    fetch("data/honorable-harvest.json").then(r => r.json()),
    fetch("data/indicator-taxa.json").then(r => r.json()),
  ]);

  appConfig = config;
  densityGrid = density;
  sightingsData = sightings;
  harvestData = harvest;
  indicatorTaxa = indicators;
}

// ── Map Init ───────────────────────────────────────────────────
function initMap() {
  const center = appConfig.default_center;
  const zoom = appConfig.default_zoom;

  map = L.map("map", {
    center: center,
    zoom: zoom,
    zoomControl: false,
    attributionControl: false,
  });

  // Dark-themed base map (good for colored overlays)
  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    maxZoom: 18,
    attribution: '&copy; <a href="https://carto.com/">CARTO</a> &copy; <a href="https://osm.org/copyright">OSM</a>',
  }).addTo(map);

  // Re-add attribution in a subtle spot
  L.control.attribution({ position: "bottomright", prefix: false }).addTo(map);

  // Zoom control on the right
  L.control.zoom({ position: "topright" }).addTo(map);
}

// ── Loading State ──────────────────────────────────────────────
function showLoading(text) {
  const banner = document.getElementById("loading-banner");
  document.getElementById("loading-text").textContent = text;
  banner.classList.remove("hidden");
}

function hideLoading() {
  document.getElementById("loading-banner").classList.add("hidden");
}

// ── Weather Fetching ───────────────────────────────────────────
async function fetchWeatherForCell(lat, lon) {
  const key = `${lat},${lon}`;
  const cached = weatherCache.get(key);
  if (cached && Date.now() - cached.fetchedAt < CACHE_TTL) {
    return cached;
  }

  const url = `${appConfig.apis.open_meteo_forecast}?latitude=${lat}&longitude=${lon}` +
    `&hourly=soil_temperature_6cm&daily=precipitation_sum` +
    `&past_days=14&forecast_days=7&timezone=auto`;

  const resp = await fetch(url);
  const data = await resp.json();

  // Current soil temp: latest non-null hourly value
  const soilTemps = (data.hourly?.soil_temperature_6cm || []).filter(v => v != null);
  const soilTemp = soilTemps.length > 0 ? soilTemps[soilTemps.length - 1] : null;

  // Soil temp 7-day average (last 168 hours)
  const recent168 = soilTemps.slice(-168);
  const soilTempAvg = recent168.length > 0
    ? recent168.reduce((a, b) => a + b, 0) / recent168.length
    : null;

  // 14-day precipitation sum (past 14 days from daily data)
  const dailyPrecip = (data.daily?.precipitation_sum || []).slice(0, 14);
  const precip14d = dailyPrecip.reduce((a, b) => a + (b || 0), 0);

  // 7-day forecast soil temps (average)
  const forecastSoil = (data.hourly?.soil_temperature_6cm || []).slice(-168);
  const forecastAvg = forecastSoil.filter(v => v != null);
  const soilTempForecast = forecastAvg.length > 0
    ? forecastAvg.reduce((a, b) => a + b, 0) / forecastAvg.length
    : null;

  // 7-day forecast precip
  const forecastPrecip = (data.daily?.precipitation_sum || []).slice(14);
  const precip7dForecast = forecastPrecip.reduce((a, b) => a + (b || 0), 0);

  // Build daily soil temp averages for forecast date estimation
  const hourlyTimes = data.hourly?.time || [];
  const hourlySoil = data.hourly?.soil_temperature_6cm || [];
  const dailySoilTemps = {};
  for (let i = 0; i < hourlyTimes.length; i++) {
    if (hourlySoil[i] == null) continue;
    const day = hourlyTimes[i].split("T")[0];
    if (!dailySoilTemps[day]) dailySoilTemps[day] = [];
    dailySoilTemps[day].push(hourlySoil[i]);
  }
  const dailySoilAvg = Object.entries(dailySoilTemps)
    .map(([date, temps]) => ({
      date,
      avg: temps.reduce((a, b) => a + b, 0) / temps.length
    }))
    .sort((a, b) => a.date.localeCompare(b.date));

  const result = {
    soilTemp,
    soilTempAvg,
    soilTempForecast,
    precip14d,
    precip7dForecast,
    dailySoilAvg,
    fetchedAt: Date.now(),
  };

  weatherCache.set(key, result);
  return result;
}

async function fetchWeatherAndRender() {
  const gridUrl = `data/grid-ma.json`;
  const gridCells = await fetch(gridUrl).then(r => r.json());

  // Fetch in batches to avoid hammering the API
  const batchSize = 20;
  const delay = ms => new Promise(r => setTimeout(r, ms));

  // Fetch weather for the user's location (or default) for the readiness gauge
  showLoading("Checking soil temperature...");
  const loc = userLocation || { lat: appConfig.default_center[0], lon: appConfig.default_center[1] };
  const userWeather = await fetchWeatherForCell(
    Math.round(loc.lat * 10) / 10,
    Math.round(loc.lon * 10) / 10
  );
  updateReadinessGauge(userWeather);

  // Show legend
  document.getElementById("legend").classList.remove("hidden");

  // Fetch visible grid cells for the probability layer
  const visibleBounds = map.getBounds();
  const visibleCells = gridCells.filter(c =>
    c.lat >= visibleBounds.getSouth() - 0.2 &&
    c.lat <= visibleBounds.getNorth() + 0.2 &&
    c.lon >= visibleBounds.getWest() - 0.2 &&
    c.lon <= visibleBounds.getEast() + 0.2
  );

  const totalCells = visibleCells.length;

  // Sample ~100 cells for fast initial render
  const sampleStep = Math.max(1, Math.floor(totalCells / 100));
  const sampledCells = visibleCells.filter((_, i) => i % sampleStep === 0);
  let completed = 0;

  showLoading(`Loading conditions... 0/${totalCells} areas`);

  for (let i = 0; i < sampledCells.length; i += batchSize) {
    const batch = sampledCells.slice(i, i + batchSize);
    await Promise.all(batch.map(c => fetchWeatherForCell(c.lat, c.lon)));
    completed += batch.length;
    showLoading(`Loading conditions... ${completed}/${totalCells} areas`);
    renderProbabilityLayer();

    if (i + batchSize < sampledCells.length) {
      await delay(200);
    }
  }

  // Fill in remaining cells in the background
  const remaining = visibleCells.filter((_, i) => i % sampleStep !== 0);
  for (let i = 0; i < remaining.length; i += batchSize) {
    const batch = remaining.slice(i, i + batchSize);
    await Promise.all(batch.map(c => fetchWeatherForCell(c.lat, c.lon)));
    completed += batch.length;

    if ((i / batchSize) % 3 === 0) {
      showLoading(`Loading conditions... ${completed}/${totalCells} areas`);
      renderProbabilityLayer();
    }
    await delay(300);
  }
  renderProbabilityLayer();
  hideLoading();
}

// ── Probability Scoring ────────────────────────────────────────
function scoreSoilTemp(tempC) {
  if (tempC == null) return 0;
  const optimal = 12;
  const sigma = 3.5;
  return Math.exp(-0.5 * Math.pow((tempC - optimal) / sigma, 2));
}

function scorePrecip(mm) {
  if (mm == null) return 0;
  const optimal = 35;
  const sigma = 15;
  return Math.exp(-0.5 * Math.pow((mm - optimal) / sigma, 2));
}

function scoreDensity(lat, lon) {
  // Find nearest density grid cell
  const gridRes = 0.1;
  const gLat = Math.round(lat / gridRes) * gridRes;
  const gLon = Math.round(lon / gridRes) * gridRes;

  const cell = densityGrid.find(c =>
    Math.abs(c.la - gLat) < 0.05 && Math.abs(c.lo - gLon) < 0.05
  );
  return cell ? cell.d : 0;
}

function computeScore(lat, lon) {
  const key = `${lat},${lon}`;
  const weather = weatherCache.get(key);
  if (!weather) return null;

  const soilScore = scoreSoilTemp(weather.soilTempAvg);
  const precipScore = scorePrecip(weather.precip14d);
  const densScore = scoreDensity(lat, lon);

  // Composite (forest cover placeholder = 0.5 since we don't have pixel-level data yet)
  const score = (
    soilScore * 0.35 +
    precipScore * 0.25 +
    densScore * 0.25 +
    0.5 * 0.15
  );

  return {
    total: Math.round(score * 100),
    soil: soilScore,
    precip: precipScore,
    density: densScore,
    soilTemp: weather.soilTempAvg,
    precip14d: weather.precip14d,
  };
}

function scoreToColor(score) {
  if (score >= 70) return "#1a9850";
  if (score >= 50) return "#91cf60";
  if (score >= 35) return "#fee08b";
  if (score >= 20) return "#fc8d59";
  return "#d73027";
}

// ── Probability Layer ──────────────────────────────────────────
function renderProbabilityLayer() {
  if (probabilityLayer) map.removeLayer(probabilityLayer);
  if (!document.getElementById("layer-probability").checked) return;

  const rectangles = [];

  weatherCache.forEach((weather, key) => {
    const [lat, lon] = key.split(",").map(Number);
    const score = computeScore(lat, lon);
    if (!score) return;

    const halfRes = 0.05;
    const bounds = [
      [lat - halfRes, lon - halfRes],
      [lat + halfRes, lon + halfRes],
    ];

    const color = scoreToColor(score.total);

    const rect = L.rectangle(bounds, {
      color: "transparent",
      fillColor: color,
      fillOpacity: 0.45,
      weight: 0,
    });

    const tempF = score.soilTemp != null ? Math.round(score.soilTemp * 9/5 + 32) : "?";
    const precipIn = (score.precip14d / 25.4).toFixed(1);

    rect.bindPopup(`
      <div class="cell-popup">
        <div class="cell-score" style="color:${color}">${score.total}/100</div>
        <h4>Morel Conditions</h4>
        <div class="cell-detail">
          Soil temp: <b>${tempF}°F</b> ${tempF >= 50 && tempF <= 61 ? '(sweet spot!)' : tempF < 50 ? '(still cool)' : '(warm)'}
        </div>
        <div class="cell-detail">
          Rain (14d): <b>${precipIn}"</b> ${score.precip14d >= 25 && score.precip14d <= 50 ? '(good)' : score.precip14d < 25 ? '(dry)' : '(wet)'}
        </div>
        <div class="cell-detail">
          Historical sightings: ${score.density > 0 ? 'Yes' : 'None recorded'}
        </div>
      </div>
    `, { maxWidth: 220 });

    rectangles.push(rect);
  });

  probabilityLayer = L.layerGroup(rectangles).addTo(map);
}

// ── Sightings Layer ────────────────────────────────────────────
function loadSightingsLayer() {
  if (!sightingsData || !document.getElementById("layer-sightings").checked) return;

  if (sightingsLayer) map.removeLayer(sightingsLayer);

  const markers = [];

  sightingsData.features.forEach(f => {
    const p = f.properties;
    const [lon, lat] = f.geometry.coordinates;

    const color = p.quality === "research" ? "#e8a838" : "#888";
    const icon = L.divIcon({
      className: "sighting-marker",
      html: `<div style="width:10px;height:10px;border-radius:50%;background:${color};border:1.5px solid #fff"></div>`,
      iconSize: [10, 10],
      iconAnchor: [5, 5],
    });

    const marker = L.marker([lat, lon], { icon });

    let popupHtml = `
      <div>
        <div class="popup-species">${p.species}</div>
        <div class="popup-date">${p.date} ${p.common ? '· ' + p.common : ''}</div>
    `;
    if (p.photo) {
      popupHtml += `<img class="popup-photo" src="${p.photo}" alt="Morel photo" loading="lazy">`;
    }
    if (p.uri) {
      popupHtml += `<a class="popup-link" href="${p.uri}" target="_blank" rel="noopener">View on iNaturalist</a>`;
    }
    popupHtml += `</div>`;
    marker.bindPopup(popupHtml, { maxWidth: 250 });

    markers.push(marker);
  });

  sightingsLayer = L.layerGroup(markers).addTo(map);
}

// ── Indicator Species Layer ────────────────────────────────────
async function loadIndicatorLayer() {
  if (indicatorLayer) map.removeLayer(indicatorLayer);
  if (!document.getElementById("layer-indicators").checked) return;

  showLoading("Finding indicator species nearby...");
  const center = map.getCenter();
  const markers = [];

  for (const taxon of indicatorTaxa) {
    try {
      const url = `${appConfig.apis.inaturalist_observations}?taxon_id=${taxon.taxon_id}` +
        `&lat=${center.lat.toFixed(2)}&lng=${center.lng.toFixed(2)}&radius=50` +
        `&d1=${thirtyDaysAgo()}&quality_grade=research,needs_id&per_page=20&order=desc&order_by=observed_on`;

      const resp = await fetch(url);
      const data = await resp.json();

      for (const obs of (data.results || [])) {
        if (!obs.geojson) continue;
        const [lon, lat] = obs.geojson.coordinates;

        const icon = L.divIcon({
          className: "indicator-icon",
          html: `<div class="indicator-icon" style="background:${taxon.color}" title="${taxon.common_name}">
            ${taxon.icon === "leaf" ? "🌿" : "🌸"}
          </div>`,
          iconSize: [24, 24],
          iconAnchor: [12, 12],
        });

        const marker = L.marker([lat, lon], { icon });
        marker.bindPopup(`
          <div>
            <div class="popup-species">${taxon.common_name}</div>
            <div class="popup-date">${obs.observed_on || "Recently"} · <i>${taxon.name}</i></div>
            <div style="font-size:12px;margin-top:4px;color:#666">${taxon.why}</div>
          </div>
        `, { maxWidth: 220 });

        markers.push(marker);
      }

      // Small delay between taxa to respect rate limits
      await new Promise(r => setTimeout(r, 500));
    } catch (err) {
      console.warn(`Failed to load ${taxon.common_name}:`, err);
    }
  }

  indicatorLayer = L.layerGroup(markers).addTo(map);
  hideLoading();
}

// ── Public Land Layer ──────────────────────────────────────────
async function loadPublicLandLayer() {
  if (publicLandLayer) map.removeLayer(publicLandLayer);
  if (!document.getElementById("layer-public-land").checked) return;

  showLoading("Loading public lands...");
  const bounds = map.getBounds();
  const envelope = `${bounds.getWest()},${bounds.getSouth()},${bounds.getEast()},${bounds.getNorth()}`;

  try {
    const url = `${appConfig.apis.massgis_openspace}?` +
      `where=1%3D1&outFields=SITE_NAME,FEE_OWNER,PUB_ACCESS&` +
      `geometry=${encodeURIComponent(envelope)}&geometryType=esriGeometryEnvelope&` +
      `inSR=4326&spatialRel=esriSpatialRelIntersects&outSR=4326&f=geojson&resultRecordCount=500`;

    const resp = await fetch(url);
    const data = await resp.json();

    publicLandLayer = L.geoJSON(data, {
      style: {
        color: "#4a7c59",
        fillColor: "#4a7c59",
        fillOpacity: 0.15,
        weight: 1,
      },
      onEachFeature: (feature, layer) => {
        const p = feature.properties || {};
        layer.bindPopup(`
          <div>
            <div class="popup-species">${p.SITE_NAME || "Conservation Land"}</div>
            <div class="popup-date">${p.FEE_OWNER || ""}</div>
            <div style="font-size:12px;margin-top:4px">
              Access: ${p.PUB_ACCESS || "Unknown"}
            </div>
          </div>
        `);
      },
    }).addTo(map);
  } catch (err) {
    console.warn("Failed to load public land:", err);
  }
  hideLoading();
}

// ── Forest Cover Layer (NLCD WMS) ──────────────────────────────
function loadForestLayer() {
  if (forestLayer) map.removeLayer(forestLayer);
  if (!document.getElementById("layer-forest").checked) return;

  forestLayer = L.tileLayer.wms(appConfig.apis.nlcd_wms, {
    layers: "NLCD_2021_Land_Cover_L48",
    format: "image/png",
    transparent: true,
    opacity: 0.35,
    attribution: "NLCD 2021",
  }).addTo(map);
}

// ── Date Estimation ─────────────────────────────────────────────
// Historical peak morel dates by latitude band (from our 49K observation analysis)
// These serve as sanity-check anchors when forecast extrapolation gives crazy results
const HISTORICAL_PEAKS = [
  { latMin: 30, latMax: 33, label: "late Feb – mid Mar", doy: 70 },
  { latMin: 33, latMax: 36, label: "mid Mar – early Apr", doy: 90 },
  { latMin: 36, latMax: 39, label: "early – mid Apr", doy: 105 },
  { latMin: 39, latMax: 41, label: "mid – late Apr", doy: 115 },
  { latMin: 41, latMax: 43, label: "late Apr – mid May", doy: 128 },
  { latMin: 43, latMax: 45, label: "early – late May", doy: 138 },
  { latMin: 45, latMax: 48, label: "mid May – early Jun", doy: 148 },
  { latMin: 48, latMax: 90, label: "late May – mid Jun", doy: 158 },
];

function getHistoricalPeak(lat) {
  for (const band of HISTORICAL_PEAKS) {
    if (lat >= band.latMin && lat < band.latMax) return band;
  }
  return HISTORICAL_PEAKS[4]; // default to 41-43 band
}

function estimateReadyDate(weather, targetTempC) {
  const daily = weather.dailySoilAvg;
  const loc = userLocation || { lat: appConfig.default_center[0] };
  const historical = getHistoricalPeak(loc.lat);

  if (!daily || daily.length < 3) {
    return `Typically ${historical.label} at this latitude`;
  }

  // Check if the 7-day forecast itself hits the sweet spot
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  for (const d of daily) {
    const date = new Date(d.date + "T12:00:00");
    const daysOut = Math.round((date - today) / (1000 * 60 * 60 * 24));
    if (d.avg >= targetTempC && daysOut > 0 && daysOut <= 7) {
      return `Forecast hits 50°F by ${formatDateShort(date)}!`;
    }
  }

  // Extrapolate from the warming trend over the full data window
  const n = daily.length;
  if (n < 4) return `Typically ${historical.label} at this latitude`;

  let sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
  for (let i = 0; i < n; i++) {
    sumX += i;
    sumY += daily[i].avg;
    sumXY += i * daily[i].avg;
    sumXX += i * i;
  }
  const slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);

  const currentTemp = daily[n - 1].avg;
  const currentTempF = Math.round(currentTemp * 9/5 + 32);
  const degreesNeededF = Math.round((targetTempC - currentTemp) * 9/5);

  // If not warming or barely warming, use historical data
  if (slope <= 0.05) {
    return `${degreesNeededF}°F to go · Typically ${historical.label} here`;
  }

  // Project days to target
  const currentFromLine = daily[n - 1].avg;
  const daysToTarget = Math.ceil((targetTempC - currentFromLine) / slope);

  if (daysToTarget <= 0) {
    return "Conditions are prime right now!";
  }

  // Sanity check: don't predict beyond late June (day 175) — morel season is over by then
  const lastDate = new Date(daily[n - 1].date + "T12:00:00");
  const targetDate = new Date(lastDate);
  targetDate.setDate(targetDate.getDate() + daysToTarget);

  const targetDOY = Math.floor((targetDate - new Date(targetDate.getFullYear(), 0, 0)) / (1000*60*60*24));
  if (targetDOY > 175) {
    // Extrapolation is unreliable — fall back to historical
    return `${degreesNeededF}°F to go · Typically ${historical.label} here`;
  }

  if (daysToTarget <= 14) {
    return `~${formatDateShort(targetDate)} (${daysToTarget} days) · ${degreesNeededF}°F to go`;
  } else {
    const weeks = Math.round(daysToTarget / 7);
    return `~${weeks} weeks out (${formatDateShort(targetDate)}) · ${degreesNeededF}°F to go`;
  }
}

function formatDateShort(date) {
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

// ── Readiness Gauge ────────────────────────────────────────────
function updateReadinessGauge(weather) {
  const label = document.getElementById("readiness-label");
  const detail = document.getElementById("readiness-detail");
  const bar = document.getElementById("readiness-bar");
  const stats = document.getElementById("readiness-stats");

  if (!weather || weather.soilTempAvg == null) {
    label.textContent = "Unable to fetch conditions";
    return;
  }

  const tempC = weather.soilTempAvg;
  const tempF = Math.round(tempC * 9/5 + 32);
  const precip = weather.precip14d;
  const precipIn = (precip / 25.4).toFixed(1);

  // Determine readiness level and estimate date
  let level, emoji, borderColor, dateEstimate = "";
  const sweetSpotC = 10; // 50°F — low end of sweet spot

  if (tempC >= 10 && tempC <= 16 && precip >= 20) {
    level = "GO TIME";
    emoji = "🟢";
    borderColor = "#1a9850";
    dateEstimate = "Conditions are prime right now!";
  } else if (tempC >= 8 && tempC <= 18 && precip >= 15) {
    level = "Looking Good";
    emoji = "🟡";
    borderColor = "#91cf60";
    dateEstimate = "Almost there — could be any day";
  } else if (tempC >= 5) {
    level = "Warming Up";
    emoji = "🟠";
    borderColor = "#fee08b";
    dateEstimate = estimateReadyDate(weather, sweetSpotC);
  } else {
    level = "Not Yet";
    emoji = "🔴";
    borderColor = "#d73027";
    dateEstimate = estimateReadyDate(weather, sweetSpotC);
  }

  label.innerHTML = `${emoji} ${level} — Soil at ${tempF}°F`;

  if (dateEstimate && !dateEstimate.startsWith("Conditions")) {
    detail.textContent = dateEstimate;
  } else if (dateEstimate) {
    detail.textContent = dateEstimate;
  } else {
    detail.textContent = `${precipIn}" rain in 14 days · Sweet spot: 50-61°F`;
  }
  bar.style.borderBottomColor = borderColor;

  // Forecast trend
  let trend = "";
  if (weather.soilTempForecast != null) {
    const diff = weather.soilTempForecast - tempC;
    if (diff > 1) trend = "↑ Warming";
    else if (diff < -1) trend = "↓ Cooling";
    else trend = "→ Steady";
  }

  stats.innerHTML = `
    <div class="stat-card">
      <div class="stat-value">${tempF}°F</div>
      <div class="stat-label">Soil Temp Now</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${precipIn}"</div>
      <div class="stat-label">Rain (14 days)</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${trend || "—"}</div>
      <div class="stat-label">7-Day Trend</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">50-61°F</div>
      <div class="stat-label">Sweet Spot</div>
    </div>
  `;
}

// ── UI Setup ───────────────────────────────────────────────────
function initUI() {
  // Readiness bar expand/collapse
  const readinessBar = document.getElementById("readiness-bar");
  const readinessPanel = document.getElementById("readiness-panel");
  readinessBar.addEventListener("click", () => {
    readinessPanel.classList.toggle("hidden");
    document.getElementById("readiness-expand").innerHTML =
      readinessPanel.classList.contains("hidden") ? "&#9662;" : "&#9652;";
  });

  // Layer panel
  document.getElementById("btn-layers").addEventListener("click", () => {
    document.getElementById("layer-panel").classList.toggle("hidden");
  });
  document.getElementById("btn-close-layers").addEventListener("click", () => {
    document.getElementById("layer-panel").classList.add("hidden");
  });

  // Layer toggles
  document.getElementById("layer-probability").addEventListener("change", renderProbabilityLayer);
  document.getElementById("layer-sightings").addEventListener("change", () => {
    if (document.getElementById("layer-sightings").checked) loadSightingsLayer();
    else if (sightingsLayer) map.removeLayer(sightingsLayer);
  });
  document.getElementById("layer-indicators").addEventListener("change", loadIndicatorLayer);
  document.getElementById("layer-public-land").addEventListener("change", loadPublicLandLayer);
  document.getElementById("layer-forest").addEventListener("change", loadForestLayer);

  // Locate button
  document.getElementById("btn-locate").addEventListener("click", () => {
    map.locate({ setView: true, maxZoom: 12 });
  });

  // Honorable Harvest
  const harvestBar = document.getElementById("harvest-bar");
  const harvestPanel = document.getElementById("harvest-panel");
  harvestBar.addEventListener("click", () => harvestPanel.classList.remove("hidden"));
  document.getElementById("btn-close-harvest").addEventListener("click", () => {
    harvestPanel.classList.add("hidden");
  });

  // Populate harvest content
  document.getElementById("harvest-attribution").textContent = harvestData.attribution;
  const principlesList = document.getElementById("harvest-principles");
  harvestData.principles.forEach(p => {
    const li = document.createElement("li");
    li.textContent = p.text;
    principlesList.appendChild(li);
  });
  const tipsList = document.getElementById("harvest-tips");
  harvestData.foraging_tips.forEach(t => {
    const li = document.createElement("li");
    li.textContent = t;
    tipsList.appendChild(li);
  });

  // Close harvest panel on backdrop click
  harvestPanel.addEventListener("click", (e) => {
    if (e.target === harvestPanel) harvestPanel.classList.add("hidden");
  });
}

// ── Helpers ────────────────────────────────────────────────────
function thirtyDaysAgo() {
  const d = new Date();
  d.setDate(d.getDate() - 30);
  return d.toISOString().split("T")[0];
}
