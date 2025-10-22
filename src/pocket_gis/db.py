from __future__ import annotations

import sqlite3
from pathlib import Path


DB_PATH = Path("data/processed/pocket_gis.db")


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

-- NYC cached crashes (last 30 days)
CREATE TABLE IF NOT EXISTS nyc_crashes (
    crash_id INTEGER PRIMARY KEY,
    severity INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    crash_date TEXT,
    wkt TEXT NOT NULL
);
CREATE VIRTUAL TABLE IF NOT EXISTS rtree_nyc_crashes USING rtree(
    crash_id, minx, maxx, miny, maxy
);
"""


def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.executescript(SCHEMA_SQL)
    return con


def ingest_nyc_crashes(con: sqlite3.Connection, gdf):
    """Persist NYC crashes into SQLite with RTREE (expects GeoDataFrame-like object)."""
    try:
        crs = getattr(gdf, "crs", None)
        gdf3857 = gdf.to_crs("EPSG:3857") if crs and gdf.crs.to_string() != "EPSG:3857" else gdf
    except Exception:
        gdf3857 = gdf
    wkts = gdf3857.geometry.to_wkt()
    rows = [
        (
            int(r[0]),  # crash_id
            int(r[1]),  # severity
            int(r[2]),  # hour
            str(r[3]) if r[3] is not None else None,  # crash_date
            w,
        )
        for r, w in zip(gdf3857[["crash_id", "severity", "hour", "crash_date"]].values, wkts)
    ]
    con.executemany(
        "INSERT OR REPLACE INTO nyc_crashes(crash_id, severity, hour, crash_date, wkt) VALUES(?,?,?,?,?)",
        rows,
    )
    b = gdf3857.geometry.bounds
    rtree_rows = [
        (int(i), float(bb.minx), float(bb.maxx), float(bb.miny), float(bb.maxy))
        for i, bb in zip(gdf3857["crash_id"], b.itertuples())
    ]
    con.executemany("INSERT OR REPLACE INTO rtree_nyc_crashes VALUES(?,?,?,?,?)", rtree_rows)
    con.commit()


def clear_nyc_cache(con: sqlite3.Connection):
    con.execute("DELETE FROM nyc_crashes")
    con.execute("DELETE FROM rtree_nyc_crashes")
    con.commit()
