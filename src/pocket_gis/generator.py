from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Tuple

import geopandas as gpd
import numpy as np
from shapely.geometry import LineString, Point
from shapely.ops import unary_union


@dataclass
class GenerationConfig:
    size: int = 10  # number of road grid cells per axis
    spacing: float = 1_000.0  # meters between parallel roads
    jitter: float = 120.0  # meters of road vertex jitter
    crash_rate: float = 0.0006  # expected crashes per meter of road
    seed: int | None = None


def _rng(seed: int | None) -> np.random.Generator:
    return np.random.default_rng(seed)


def generate_roads(cfg: GenerationConfig) -> gpd.GeoDataFrame:
    """Generate a grid-like road network with slight jitter.

    Returns columns: road_id (int), class (str), length_m (float), geometry (LineString)
    """
    rng = _rng(cfg.seed)
    lines = []
    classes = []

    # Create orthogonal grid lines with small jitter
    for i in range(cfg.size + 1):
        y = i * cfg.spacing + rng.normal(0, cfg.jitter)
        x0, x1 = 0.0, cfg.size * cfg.spacing
        lines.append(LineString([(x0, y), (x1, y)]))
        classes.append("collector" if i % 3 else "arterial")

    for j in range(cfg.size + 1):
        x = j * cfg.spacing + rng.normal(0, cfg.jitter)
        y0, y1 = 0.0, cfg.size * cfg.spacing
        lines.append(LineString([(x, y0), (x, y1)]))
        classes.append("collector" if j % 3 else "arterial")

    gdf = gpd.GeoDataFrame({"class": classes}, geometry=lines, crs="EPSG:3857")

    # Ensure valid linework; compute lengths directly on LineStrings
    # Note: buffer(0) is for polygons and can corrupt LineStrings; avoid here.
    gdf["length_m"] = gdf.geometry.length
    gdf.insert(0, "road_id", np.arange(1, len(gdf) + 1, dtype=int))
    return gdf


def generate_crashes(roads: gpd.GeoDataFrame, cfg: GenerationConfig) -> gpd.GeoDataFrame:
    """Poisson-sample crash points along roads with severity and time attributes."""
    rng = _rng(cfg.seed)
    total_length = float(roads.length_m.sum())
    expected = total_length * cfg.crash_rate
    n = rng.poisson(max(expected, 1.0))

    # Sample uniformly along road segments proportional to length
    lengths = roads.length_m.values
    probs = lengths / lengths.sum()
    road_choice = rng.choice(len(roads), size=n, p=probs)

    pts = []
    severity = []
    hour = []
    for idx in road_choice:
        line: LineString = roads.geometry.iloc[idx]
        t = rng.random()
        point = line.interpolate(t, normalized=True)
        pts.append(Point(point.x + rng.normal(0, 10), point.y + rng.normal(0, 10)))
        # Severity 1-5, skewed heavy-tail
        sev = int(np.clip(np.round(rng.pareto(1.3) + 1), 1, 5))
        severity.append(sev)
        hour.append(int(rng.integers(0, 24)))

    gdf = gpd.GeoDataFrame({
        "severity": severity,
        "hour": hour,
    }, geometry=pts, crs=roads.crs)

    gdf.insert(0, "crash_id", np.arange(1, len(gdf) + 1, dtype=int))
    return gdf


def export_layers(roads: gpd.GeoDataFrame, crashes: gpd.GeoDataFrame, out_dir: str) -> Tuple[str, str]:
    out_roads = f"{out_dir}/roads.geojson"
    out_crashes = f"{out_dir}/crashes.geojson"
    roads.to_file(out_roads, driver="GeoJSON")
    crashes.to_file(out_crashes, driver="GeoJSON")
    return out_roads, out_crashes
