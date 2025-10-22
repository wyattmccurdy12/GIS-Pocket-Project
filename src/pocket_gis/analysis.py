from __future__ import annotations

import math
from dataclasses import dataclass

import geopandas as gpd
import numpy as np
from shapely.geometry import LineString


@dataclass
class AnalysisConfig:
    search_radius: float = 80.0  # meters for nearest-road assignment


def nearest_road(crashes: gpd.GeoDataFrame, roads: gpd.GeoDataFrame, cfg: AnalysisConfig) -> gpd.GeoDataFrame:
    # Use robust nearest-neighbor join with optional search radius
    joined = gpd.sjoin_nearest(
        crashes,
        roads[["road_id", "geometry"]],
        how="left",
        max_distance=cfg.search_radius,
        distance_col="dist_m",
    )

    # Rename columns from right frame to avoid suffixes
    # After sjoin_nearest with crashes as left, 'road_id' comes from right
    out = joined[["crash_id", "severity", "hour", "geometry", "road_id", "dist_m"]].copy()
    return out


def road_summary(roads: gpd.GeoDataFrame, crash_assignments: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Aggregate crashes per road
    grp = crash_assignments.dropna(subset=["road_id"]).groupby("road_id")
    agg = grp.agg(n_crashes=("crash_id", "count"),
                  sev_sum=("severity", "sum"))

    summary = roads.merge(agg, on="road_id", how="left")
    summary["n_crashes"] = summary["n_crashes"].fillna(0)
    summary["sev_sum"] = summary["sev_sum"].fillna(0)

    # Simple risk score scaled by length
    summary["crashes_per_km"] = (summary["n_crashes"] / (summary["length_m"] / 1000)).replace([np.inf, -np.inf], 0)
    summary["sev_per_km"] = (summary["sev_sum"] / (summary["length_m"] / 1000)).replace([np.inf, -np.inf], 0)
    summary["risk_score"] = 0.6 * summary["sev_per_km"] + 0.4 * summary["crashes_per_km"]

    return summary


def run_qaqc(roads: gpd.GeoDataFrame, crashes: gpd.GeoDataFrame) -> dict:
    issues = {}
    # Basic checks
    issues["roads_null_geom"] = int(roads.geometry.isna().sum())
    issues["crashes_null_geom"] = int(crashes.geometry.isna().sum())
    issues["roads_invalid"] = int((~roads.is_valid).sum())
    issues["crashes_invalid"] = int((~crashes.is_valid).sum())

    return issues
