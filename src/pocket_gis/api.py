from __future__ import annotations

import os
import sqlite3
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

from .db import DB_PATH, init_db, ingest_nyc_crashes, clear_nyc_cache
from .nyc import fetch_nyc_crashes_one_month, to_geodataframe, kmeans_hotspots

app = Flask(__name__)
CORS(app, resources={r"/nyc/*": {"origins": "*"}})

# NYC bbox in EPSG:3857 (approx): x[-8270000,-8205000], y[4960000,5030000]
NYC_BBOX_3857 = (-8270000.0, -8205000.0, 4960000.0, 5030000.0)


def get_con() -> sqlite3.Connection:
    # Ensure DB path and schema exist; safe to call repeatedly
    con = init_db(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


## NYC-only API


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/nyc/crashes")
def nyc_crashes():
    limit = int(request.args.get("limit", 5000))
    hour = request.args.get("hour")
    refresh = request.args.get("refresh") == "1"

    with get_con() as con:
        if refresh:
            df = fetch_nyc_crashes_one_month(limit=limit)
            gdf = to_geodataframe(df)
            clear_nyc_cache(con)
            ingest_nyc_crashes(con, gdf)
        # Query from cache (optionally filter by hour) and clip to NYC bbox
        xmin, xmax, ymin, ymax = NYC_BBOX_3857
        if hour is not None:
            rows = con.execute(
                """
                SELECT c.crash_id, c.severity, c.hour, c.crash_date, c.wkt
                FROM nyc_crashes c
                JOIN rtree_nyc_crashes r ON r.crash_id = c.crash_id
                WHERE c.wkt != '' AND c.hour = ?
                  AND r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ?
                LIMIT ?
                """,
                (int(hour), xmax, xmin, ymax, ymin, limit),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT c.crash_id, c.severity, c.hour, c.crash_date, c.wkt
                FROM nyc_crashes c
                JOIN rtree_nyc_crashes r ON r.crash_id = c.crash_id
                WHERE c.wkt != ''
                  AND r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ?
                LIMIT ?
                """,
                (xmax, xmin, ymax, ymin, limit),
            ).fetchall()
    return jsonify(_rows_to_features(rows, ["crash_id", "severity", "hour", "crash_date"]))


@app.get("/nyc/hotspots")
def nyc_hotspots():
    k = int(request.args.get("k", 20))
    limit = int(request.args.get("limit", 5000))
    hour = request.args.get("hour")
    refresh = request.args.get("refresh") == "1"

    with get_con() as con:
        if refresh:
            df = fetch_nyc_crashes_one_month(limit=limit)
            gdf = to_geodataframe(df)
            clear_nyc_cache(con)
            ingest_nyc_crashes(con, gdf)
        xmin, xmax, ymin, ymax = NYC_BBOX_3857
        if hour is not None:
            rows = con.execute(
                """
                SELECT c.crash_id, c.severity, c.hour, c.wkt
                FROM nyc_crashes c
                JOIN rtree_nyc_crashes r ON r.crash_id = c.crash_id
                WHERE c.wkt != '' AND c.hour = ?
                  AND r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ?
                LIMIT ?
                """,
                (int(hour), xmax, xmin, ymax, ymin, limit),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT c.crash_id, c.severity, c.hour, c.wkt
                FROM nyc_crashes c
                JOIN rtree_nyc_crashes r ON r.crash_id = c.crash_id
                WHERE c.wkt != ''
                  AND r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ?
                LIMIT ?
                """,
                (xmax, xmin, ymax, ymin, limit),
            ).fetchall()
    # Convert rows to GDF (EPSG:3857) then compute hotspots
    # Build a minimal GeoDataFrame from rows
    import shapely.wkt as swkt
    import geopandas as gpd
    from shapely.geometry import Point
    geoms = []
    sev = []
    hr = []
    for r in rows:
        crash_id, s, h, wkt = r
        geom = swkt.loads(wkt) if wkt else None
        geoms.append(geom)
        sev.append(int(s))
        hr.append(int(h))
    gdf = gpd.GeoDataFrame({"severity": sev, "hour": hr}, geometry=geoms, crs="EPSG:3857")
    hs = kmeans_hotspots(gdf.to_crs("EPSG:3857"), k=k)
    return jsonify(_gdf_to_fc(hs))

@app.get("/nyc/timeseries")
def nyc_timeseries():
    """Return daily crash counts within a selected area for the last 30 days.

    Query params:
      - mode: 'bbox' or 'point'
      - bbox: "minlon,minlat,maxlon,maxlat" (EPSG:4326) when mode=bbox
      - lon, lat: point in EPSG:4326 when mode=point
      - radius_m: buffer radius in meters for point mode (default 250)
    """
    from datetime import datetime, timedelta, UTC
    from shapely.geometry import box, Point
    import shapely.wkt as swkt
    from pyproj import Transformer

    mode = request.args.get("mode", "bbox")
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

    # Build filter geometry in EPSG:3857
    geom_filter = None
    xmin = xmax = ymin = ymax = None
    try:
        if mode == "bbox":
            bbox = request.args.get("bbox")
            if not bbox:
                return jsonify({"error": "bbox required"}), 400
            minlon, minlat, maxlon, maxlat = [float(v) for v in bbox.split(",")]
            x1, y1 = transformer.transform(minlon, minlat)
            x2, y2 = transformer.transform(maxlon, maxlat)
            xmin, xmax = (min(x1, x2), max(x1, x2))
            ymin, ymax = (min(y1, y2), max(y1, y2))
            from shapely.geometry import box as sbox
            geom_filter = sbox(xmin, ymin, xmax, ymax)
        elif mode == "point":
            lon = float(request.args.get("lon"))
            lat = float(request.args.get("lat"))
            radius_m = float(request.args.get("radius_m", 250))
            x, y = transformer.transform(lon, lat)
            xmin, xmax = x - radius_m, x + radius_m
            ymin, ymax = y - radius_m, y + radius_m
            geom_filter = Point(x, y).buffer(radius_m)
        else:
            return jsonify({"error": "invalid mode"}), 400
    except Exception:
        return jsonify({"error": "invalid geometry parameters"}), 400

    # Candidate rows via RTREE bbox, then precise spatial filter
    with get_con() as con:
        rows = con.execute(
            """
            SELECT c.crash_date, c.wkt
            FROM nyc_crashes c
            JOIN rtree_nyc_crashes r ON r.crash_id = c.crash_id
            WHERE c.wkt != ''
              AND r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ?
            """,
            (xmax, xmin, ymax, ymin),
        ).fetchall()

    dates = []
    for crash_date, wkt in rows:
        try:
            geom = swkt.loads(wkt)
            if geom is not None and geom_filter.intersects(geom):
                dates.append(crash_date)
        except Exception:
            continue

    # Build last 30 days date range
    today = datetime.now(UTC).date()
    start = today - timedelta(days=29)
    day_index = {}
    series = []
    for i in range(30):
        d = (start + timedelta(days=i)).isoformat()
        day_index[d] = i
        series.append({"date": d, "count": 0})
    total = 0
    for d in dates:
        if not d:
            continue
        # d is stored as string 'YYYY-MM-DD'
        if d in day_index:
            series[day_index[d]]["count"] += 1
            total += 1

    return jsonify({
        "mode": mode,
        "series": series,
        "total": total,
    })

@app.get("/nyc/summary")
def nyc_summary():
    """Return severity histogram and summary stats within selected area for last 30 days."""
    from shapely.geometry import Point
    import shapely.wkt as swkt
    from pyproj import Transformer

    mode = request.args.get("mode", "bbox")
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

    # Build filter geometry in EPSG:3857
    geom_filter = None
    xmin = xmax = ymin = ymax = None
    try:
        if mode == "bbox":
            bbox = request.args.get("bbox")
            if not bbox:
                return jsonify({"error": "bbox required"}), 400
            minlon, minlat, maxlon, maxlat = [float(v) for v in bbox.split(",")]
            x1, y1 = transformer.transform(minlon, minlat)
            x2, y2 = transformer.transform(maxlon, maxlat)
            xmin, xmax = (min(x1, x2), max(x1, x2))
            ymin, ymax = (min(y1, y2), max(y1, y2))
            from shapely.geometry import box as sbox
            geom_filter = sbox(xmin, ymin, xmax, ymax)
        elif mode == "point":
            lon = float(request.args.get("lon"))
            lat = float(request.args.get("lat"))
            radius_m = float(request.args.get("radius_m", 250))
            x, y = transformer.transform(lon, lat)
            xmin, xmax = x - radius_m, x + radius_m
            ymin, ymax = y - radius_m, y + radius_m
            geom_filter = Point(x, y).buffer(radius_m)
        else:
            return jsonify({"error": "invalid mode"}), 400
    except Exception:
        return jsonify({"error": "invalid geometry parameters"}), 400

    # Candidate rows via RTREE bbox, then precise spatial filter
    with get_con() as con:
        rows = con.execute(
            """
            SELECT c.severity, c.crash_date, c.wkt
            FROM nyc_crashes c
            JOIN rtree_nyc_crashes r ON r.crash_id = c.crash_id
            WHERE c.wkt != ''
              AND r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ?
            """,
            (xmax, xmin, ymax, ymin),
        ).fetchall()

    # Aggregate
    hist = {str(i): 0 for i in range(1, 6)}
    total = 0
    dates = []
    sev_sum = 0
    for s, d, wkt in rows:
        try:
            geom = swkt.loads(wkt)
            if geom is not None and geom_filter.intersects(geom):
                sev = int(s)
                hist[str(sev)] = hist.get(str(sev), 0) + 1
                sev_sum += sev
                total += 1
                if d:
                    dates.append(d)
        except Exception:
            continue

    avg_sev = (sev_sum / total) if total else 0.0
    min_date = min(dates) if dates else None
    max_date = max(dates) if dates else None

    return jsonify({
        "severity_hist": hist,
        "total": total,
        "avg_severity": round(avg_sev, 2),
        "min_date": min_date,
        "max_date": max_date,
    })

def _rows_to_features(rows, prop_names):
    import shapely.wkt as swkt
    from shapely.geometry import mapping
    from shapely.ops import transform
    from pyproj import Transformer

    # SQLite stores WKT in EPSG:3857; Leaflet expects EPSG:4326
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True).transform

    feats = []
    for row in rows:
        *props_vals, wkt = row
        geom_obj = swkt.loads(wkt) if wkt else None
        geom_4326 = transform(transformer, geom_obj) if geom_obj is not None else None
        geom = mapping(geom_4326) if geom_4326 is not None else None
        props = {k: v for k, v in zip(prop_names, props_vals)}
        feats.append({
            "type": "Feature",
            "properties": props,
            "geometry": geom,
        })
    return {"type": "FeatureCollection", "features": feats}


def _gdf_to_fc(gdf):
    from shapely.geometry import mapping
    feats = []
    if gdf is None or len(gdf) == 0:
        return {"type": "FeatureCollection", "features": feats}
    # Ensure WGS84 for Leaflet
    try:
        if getattr(gdf, "crs", None) is not None and gdf.crs.to_string() != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")
    except Exception:
        pass
    geom_col = gdf.geometry.name
    for _, row in gdf.iterrows():
        props = {k: v for k, v in row.items() if k != geom_col}
        geom = mapping(row[geom_col]) if row[geom_col] is not None else None
        feats.append({"type": "Feature", "properties": props, "geometry": geom})
    return {"type": "FeatureCollection", "features": feats}


if __name__ == "__main__":
    app.run(port=int(os.getenv("PORT", "5000")))
