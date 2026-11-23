"""BQ3 road distances: OpenStreetMap road network (OSRM) from listings to places of work/study.

Straight-line distance from a district centre is hard to read and hides how
large a district can be (Thành phố Thủ Đức is over 200 km²). BQ3 therefore
measures the road distance from every listing to each named place of work or
study.

Listings are grouped on a grid of 0.01° (about 1.1 km): a listing at (lat, lon)
belongs to cell (floor(lat * 100 + 0.5), floor(lon * 100 + 0.5)) and the route
starts at the cell centre, so the offset inside a cell is below ~0.8 km.

The network call is kept out of the pipeline. ``fetch`` is run by a person on
the host while Trino is open (read window) and writes
``config/commute_road_distance.csv``; the Gold job only reads that file, so a
pipeline run never depends on an external service. Cells that appear in later
batches have no road distance until ``fetch`` is run again; the Gold summary
reports that coverage.

Source: OSRM public server (router.project-osrm.org), profile ``driving``, map
data © OpenStreetMap contributors (ODbL). Minutes are free-flow car times: a
lower bound at rush hour, and motorbikes may use shorter alleys.

Run (host, read window): python -m src.gold.commute_road fetch
"""

from __future__ import annotations

import csv
import json
import math
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

GRID = 100  # cells per degree (0.01°)
ROAD_PATH = PROJECT_ROOT / "config/commute_road_distance.csv"
FETCH_SUMMARY = PROJECT_ROOT / "docs/validation/commute_road_fetch.json"
OSRM_URL = "https://router.project-osrm.org/table/v1/driving/"
OSRM_SOURCE = "OSRM public server router.project-osrm.org, profile driving; map data © OpenStreetMap contributors (ODbL)"
MAX_COORDINATES = 100  # sources + destinations per table request
USER_AGENT = "tlcn-lakehouse-bq3/1.0 (academic project)"

# Road-distance rings (upper bound in km, label). Labels sort in reading order.
RINGS: tuple[tuple[float | None, str], ...] = (
    (5, "1. Dưới 5 km"), (10, "2. 5–10 km"), (15, "3. 10–15 km"), (20, "4. 15–20 km"), (30, "5. 20–30 km"), (None, "6. Từ 30 km"),
)
FAR_COMPARISON_RING = "3. 10–15 km"  # "how much cheaper 10–15 km away than within 5 km"
# A route starts at the cell centre (≤ ~0.8 km from the listing) and OSRM snaps to the nearest road,
# so road_km may fall slightly below the listing's straight-line distance.
ROAD_CELL_TOLERANCE_KM = 1.5


def cell_index(value: float) -> int:
    """Grid index of a latitude or longitude (round half up, same as the Spark/Trino expression)."""

    return math.floor(value * GRID + 0.5)


def cell_center(index: int) -> float:
    return index / GRID


def cell_sql(column: str) -> str:
    """SQL expression (Spark and Trino) of ``cell_index`` for a coordinate column."""

    return f"CAST(floor({column} * {GRID} + 0.5) AS INT)"


def ring_label(km: float | None) -> str | None:
    if km is None:
        return None
    for upper, label in RINGS:
        if upper is None or km < upper:
            return label
    return None


def ring_sql(column: str) -> str:
    """SQL CASE expression of ``ring_label`` (NULL distance stays NULL)."""

    parts = [f"WHEN {column} < {upper} THEN '{label}'" for upper, label in RINGS if upper is not None]
    return f"CASE WHEN {column} IS NULL THEN NULL {' '.join(parts)} ELSE '{RINGS[-1][1]}' END"


def load_road_distances(path: Path = ROAD_PATH) -> list[dict[str, Any]]:
    """Rows of the precomputed file: destination_id, lat_idx, lon_idx, road_km, road_min."""

    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        return [{"destination_id": r["destination_id"], "lat_idx": int(r["lat_idx"]), "lon_idx": int(r["lon_idx"]),
                 "road_km": float(r["road_km"]), "road_min": float(r["road_min"])} for r in csv.DictReader(handle)]


def chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def table_request(destinations: list[dict[str, Any]], cells: list[tuple[int, int]], get=None) -> list[list[tuple[float, float] | None]]:
    """One OSRM table call: for each cell, (road_km, road_min) to each destination, or None when unroutable."""

    import requests

    get = get or requests.get
    coordinates = [f"{d['lon']:.6f},{d['lat']:.6f}" for d in destinations]
    coordinates += [f"{cell_center(lon):.2f},{cell_center(lat):.2f}" for lat, lon in cells]
    n_dest = len(destinations)
    params = {"sources": ";".join(str(n_dest + i) for i in range(len(cells))),
              "destinations": ";".join(str(i) for i in range(n_dest)), "annotations": "distance,duration"}
    for attempt in range(4):
        response = get(OSRM_URL + ";".join(coordinates), params=params, headers={"User-Agent": USER_AGENT}, timeout=60)
        if response.status_code == 200 and response.json().get("code") == "Ok":
            data = response.json()
            return [[None if dist is None or dur is None else (round(dist / 1000, 3), round(dur / 60, 1))
                     for dist, dur in zip(data["distances"][i], data["durations"][i])] for i in range(len(cells))]
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"OSRM table request failed: HTTP {response.status_code} {response.text[:200]}")


def listing_cells() -> dict[str, list[tuple[int, int]]]:
    """Grid cells of representative sale listings with coordinates, by province (Trino, read window)."""

    import trino

    cursor = trino.dbapi.connect(host="localhost", port=8090, user="bq3_road", catalog="iceberg").cursor()
    cursor.execute(f"""
        SELECT d.province_name, {cell_sql('l.lat')} AS lat_idx, {cell_sql('l.lon')} AS lon_idx
        FROM gold.fact_listing f
        JOIN gold.dim_location d ON d.location_key = f.location_key
        JOIN silver.listing_location l ON l.source_id = f.source_id
        WHERE f.is_dup_representative AND l.has_coord AND f.price > 0 AND f.area > 0
        GROUP BY 1, 2, 3""")
    cells: dict[str, list[tuple[int, int]]] = {}
    for province, lat_idx, lon_idx in cursor.fetchall():
        cells.setdefault(province, []).append((int(lat_idx), int(lon_idx)))
    return {province: sorted(values) for province, values in cells.items()}


def fetch() -> dict[str, Any]:
    from src.gold.gold_rules import load_destinations
    from src.silver.build_location import haversine_km

    named = [d for d in load_destinations() if not d["destination_id"].startswith("center_")]
    by_province: dict[str, list[dict[str, Any]]] = {}
    for d in named:
        by_province.setdefault(d["province_name"], []).append(d)
    cells = listing_cells()
    rows, unroutable, requests_made = [], 0, 0
    ratios: dict[str, list[float]] = {}
    for province, destinations in sorted(by_province.items()):
        province_cells = cells.get(province, [])
        for batch in chunks(province_cells, MAX_COORDINATES - len(destinations)):
            result = table_request(destinations, batch)
            requests_made += 1
            for (lat_idx, lon_idx), per_destination in zip(batch, result):
                for dest, value in zip(destinations, per_destination):
                    if value is None:
                        unroutable += 1
                        continue
                    road_km, road_min = value
                    rows.append((dest["destination_id"], lat_idx, lon_idx, road_km, road_min))
                    straight = haversine_km(cell_center(lat_idx), cell_center(lon_idx), dest["lat"], dest["lon"])
                    if straight >= 2:
                        ratios.setdefault(dest["destination_id"], []).append(road_km / straight)
            time.sleep(1.1)  # public server: at most one request per second
    rows.sort()
    with ROAD_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["destination_id", "lat_idx", "lon_idx", "road_km", "road_min"])
        writer.writerows(rows)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": OSRM_SOURCE,
        "grid_degrees": 1 / GRID,
        "destinations": len(named),
        "cells_by_province": {p: len(cells.get(p, [])) for p in sorted(by_province)},
        "requests": requests_made,
        "pairs": len(rows),
        "unroutable_pairs": unroutable,
        "median_road_over_straight": {k: round(statistics.median(v), 3) for k, v in sorted(ratios.items())},
        "output": str(ROAD_PATH.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "status": "PASS" if rows else "FAIL",
    }
    FETCH_SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> None:
    if sys.argv[1:] != ["fetch"]:
        raise SystemExit("Usage: python -m src.gold.commute_road fetch")
    print(json.dumps(fetch(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
