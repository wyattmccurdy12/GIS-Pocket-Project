from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd

from src.pocket_gis.generator import GenerationConfig, generate_roads, generate_crashes
from src.pocket_gis.db import init_db, ingest
from src.pocket_gis.analysis import AnalysisConfig, nearest_road, road_summary
from src.pocket_gis.io import write_geojson


def main():
    ap = argparse.ArgumentParser(description="Pocket GIS: generate, build DB, analyze")
    ap.add_argument("--size", type=int, default=10)
    ap.add_argument("--seed", type=int, default=123)
    ap.add_argument("--out", type=str, default="data/processed")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Generate synthetic data
    gcfg = GenerationConfig(size=args.size, seed=args.seed)
    roads = generate_roads(gcfg)
    crashes = generate_crashes(roads, gcfg)

    # Export raw layers
    write_geojson(roads, out_dir / "roads.geojson")
    write_geojson(crashes, out_dir / "crashes.geojson")

    # 2) Create DB and ingest
    con = init_db()
    ingest(con, roads, crashes)

    # 3) Analysis pipeline
    acfg = AnalysisConfig()
    crash_assign = nearest_road(crashes, roads, acfg)
    summary = road_summary(roads, crash_assign)

    # Persist summary in a SQL table for the API
    # Store key metrics along with geometry WKT
    summary[["road_id", "class", "length_m", "n_crashes", "sev_sum", "crashes_per_km", "sev_per_km", "risk_score"]]
    summary.to_file(out_dir / "road_summary.geojson", driver="GeoJSON")

    with con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS road_summary (
            road_id INTEGER PRIMARY KEY,
            n_crashes REAL,
            sev_sum REAL,
            crashes_per_km REAL,
            sev_per_km REAL,
            risk_score REAL
        )
        """)
        con.executemany(
            "INSERT OR REPLACE INTO road_summary VALUES(?,?,?,?,?,?)",
            [
                (
                    int(r.road_id),
                    float(r.n_crashes),
                    float(r.sev_sum),
                    float(r.crashes_per_km),
                    float(r.sev_per_km),
                    float(r.risk_score),
                )
                for _, r in summary.iterrows()
            ],
        )

    print("Pipeline complete:")
    print(f"  Roads: {len(roads)}")
    print(f"  Crashes: {len(crashes)}")
    print(f"  DB: data/processed/pocket_gis.db")
    print(f"  Summary features: {len(summary)}")


if __name__ == "__main__":
    main()
