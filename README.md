# Pocket GIS: NYC Crash Hotspots

A compact NYC-focused GIS app that showcases Python, SQL, GeoPandas, Shapely, and NumPy for transportation safety analytics.

It fetches the latest 30 days of NYC crash data, caches it in SQLite with RTREE, computes simple crash hotspots, and visualizes them on a Leaflet web map.

## Features
- NYC data ingestion from NYC Open Data (Socrata `h9gi-nx95`)
- SQLite cache with WKT geometry and RTREE bounding-box index
- Hotspot clustering (k-means) and hour-of-day filtering
- Leaflet web map UI with count-sized hotspot markers

## Quick start

1) Create a Python environment and install deps

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

2) Run the web app

```bash
FLASK_APP=src/pocket_gis/api.py flask run -p 5000
```

Open http://127.0.0.1:5000 — the map auto-loads the latest 30 days of NYC data.

Controls:
- Hotspots k: number of clusters
- Hour: optional hour-of-day filter (0–23)
- Refresh NYC: force re-fetch and refresh cache
 - Timeseries: use the map’s draw tools (rectangle or point) to view daily crash counts for the last 30 days in the selected area. For point mode, adjust the radius slider (100–1000 m).

## Endpoints
- `/nyc/crashes?limit=5000&hour=2&refresh=1`
- `/nyc/hotspots?k=20&hour=14&refresh=1`
 - `/nyc/timeseries?mode=bbox&bbox=minlon,minlat,maxlon,maxlat`
 - `/nyc/timeseries?mode=point&lon=-73.98&lat=40.75&radius_m=250`


## Project layout

- `src/pocket_gis/nyc.py` – NYC API client and conversion to GeoDataFrame
- `src/pocket_gis/db.py` – SQLite cache and RTREE index for NYC crashes
- `src/pocket_gis/api.py` – Flask API and Leaflet web UI
- `tests/test_smoke.py` – Basic app smoke test

## Notes
- All GeoJSON returned is in WGS84 (EPSG:4326) for Leaflet compatibility.
- For larger-scale analytics, swapping SQLite for PostGIS is straightforward.
