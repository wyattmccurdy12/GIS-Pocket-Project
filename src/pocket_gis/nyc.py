from __future__ import annotations

import math
from datetime import datetime, timedelta, UTC
from typing import Tuple, List

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from shapely.geometry import Point

NYC_API = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

# NYC CRS is typically EPSG:2263 (NAD83 / New York Long Island ftUS),
# but we'll keep everything in Web Mercator (EPSG:3857) for simplicity.


def fetch_nyc_crashes_one_month(limit: int = 5000) -> pd.DataFrame:
    now = datetime.now(UTC)
    start = now - timedelta(days=30)
    # Socrata SODA query: filter for one month and with coordinates
    # Use $select and $where to constrain; request latitude/longitude and key fields
    # NYC bounding box approx: lat 40.4774–40.9176, lon -74.2591–-73.7004
    params = {
        "$select": "collision_id, crash_date, crash_time, latitude, longitude, number_of_persons_injured, number_of_persons_killed",
        "$where": (
            f"crash_date between '{start.date()}' and '{now.date()}' and "
            "latitude is not null and longitude is not null and "
            "latitude <> 0 and longitude <> 0 and "
            "latitude between 40.4774 and 40.9176 and longitude between -74.2591 and -73.7004"
        ),
        "$limit": str(limit),
        "$order": "crash_date DESC",
    }
    r = requests.get(NYC_API, params=params, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    if df.empty:
        return df

    # Normalize types
    for col in ["number_of_persons_injured", "number_of_persons_killed"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    # Extract hour from crash_time if present
    if "crash_time" in df.columns:
        df["hour"] = pd.to_datetime(df["crash_time"], format="%H:%M", errors="coerce").dt.hour.fillna(0).astype(int)
    else:
        df["hour"] = 0

    # Severity proxy: 1 + injured + 5*killed (bounded 1..5)
    severity = 1 + df.get("number_of_persons_injured", 0) + 5 * df.get("number_of_persons_killed", 0)
    df["severity"] = np.clip(severity, 1, 5)

    return df


def to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    if df.empty:
        return gpd.GeoDataFrame(columns=["crash_id", "severity", "hour", "geometry"], geometry="geometry", crs="EPSG:3857")
    df = df.copy()
    df["crash_id"] = pd.to_numeric(df.get("collision_id"), errors="coerce").fillna(0).astype(int)
    # Drop any remaining out-of-bounds points just in case
    df = df[(df["latitude"].astype(float).between(40.4774, 40.9176)) & (df["longitude"].astype(float).between(-74.2591, -73.7004))]
    pts = [Point(float(lon), float(lat)) for lon, lat in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=pts, crs="EPSG:4326")
    gdf = gdf.to_crs("EPSG:3857")
    # Keep crash_date for caching/persistence
    gdf["crash_date"] = pd.to_datetime(gdf.get("crash_date"), errors="coerce").dt.date.astype("string")
    return gdf[["crash_id", "severity", "hour", "crash_date", "geometry"]]


def kmeans_hotspots(crashes: gpd.GeoDataFrame, k: int = 20) -> gpd.GeoDataFrame:
    if crashes.empty:
        return gpd.GeoDataFrame(columns=["cluster", "n", "severity_mean", "geometry"], geometry="geometry", crs=crashes.crs)
    # Simple kmeans on planar 3857 coords
    crashes = crashes[crashes.geometry.notna()]
    if crashes.empty:
        return gpd.GeoDataFrame(columns=["cluster", "n", "severity_mean", "geometry"], geometry="geometry", crs=crashes.crs)
    X = np.array([[geom.x, geom.y] for geom in crashes.geometry])
    # Initialize centroids randomly
    rng = np.random.default_rng(42)
    centroids = X[rng.choice(len(X), size=min(k, len(X)), replace=False)]
    for _ in range(10):
        # Assign
        d2 = ((X[:, None, :] - centroids[None, :, :]) ** 2).sum(axis=2)
        labels = d2.argmin(axis=1)
        # Update
        for i in range(centroids.shape[0]):
            pts = X[labels == i]
            if len(pts):
                centroids[i] = pts.mean(axis=0)
    # Build cluster stats
    labs = labels
    stats = []
    geoms = []
    for i in range(centroids.shape[0]):
        mask = labs == i
        n = int(mask.sum())
        if n == 0:
            continue
        sev_mean = float(crashes.loc[mask, "severity"].mean())
        stats.append((i, n, sev_mean))
        cx, cy = centroids[i]
        geoms.append(Point(cx, cy))
    out = gpd.GeoDataFrame(stats, columns=["cluster", "n", "severity_mean"], geometry=geoms, crs=crashes.crs)
    return out.sort_values("n", ascending=False).reset_index(drop=True)
