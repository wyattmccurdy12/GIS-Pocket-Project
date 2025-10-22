from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import geopandas as gpd
from shapely.geometry import mapping


def gdf_to_feature_collection(gdf: gpd.GeoDataFrame) -> dict:
    features = []
    for idx, row in gdf.iterrows():
        props = {k: v for k, v in row.items() if k != gdf.geometry.name}
        geom = mapping(row.geometry) if row.geometry is not None else None
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": geom,
        })
    return {"type": "FeatureCollection", "features": features}


essential_default = {
    "indent": 2,
}


def write_geojson(gdf: gpd.GeoDataFrame, path: str | Path, **dump_kwargs) -> str:
    fc = gdf_to_feature_collection(gdf)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fc, f, **(essential_default | dump_kwargs))
    return str(path)
