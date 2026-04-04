"""
Loitering anomaly detection (Anomaly B)
Adaptive grid sizing for geographic clustering.
"""

from typing import List, Tuple, Dict, Any, Set
from config import (
    LOITERING_PROXIMITY_KM, LOITERING_SOG_KNOTS,
    LOITERING_DURATION_HOURS
)
from geo import haversine_distance


def detect_loitering_anomalies(
    mmsi_records: Dict[str, List[Tuple]],
    proximity_threshold_km: float = LOITERING_PROXIMITY_KM,
    sog_threshold_knots: float = LOITERING_SOG_KNOTS,
    loitering_duration_hours: float = LOITERING_DURATION_HOURS,
) -> List[Dict[str, Any]]:

    print("[Loitering] Starting ultra-fast screening...")

    anomalies = []
    loitering_sec = loitering_duration_hours * 3600

    if len(mmsi_records) < 2:
        return anomalies

    # --------------------------------------------------
    # FILTER 1: slow vessels
    # --------------------------------------------------
    slow_vessels_data = {}

    for mmsi, records in mmsi_records.items():
        slow_records = [
            (ts, epoch, lat, lon, sog, draught)
            for ts, epoch, lat, lon, sog, draught in records
            if sog <= sog_threshold_knots
        ]

        if len(slow_records) >= 2:
            avg_lat = sum(r[2] for r in slow_records) / len(slow_records)
            avg_lon = sum(r[3] for r in slow_records) / len(slow_records)
            slow_vessels_data[mmsi] = (slow_records, avg_lat, avg_lon)

    if len(slow_vessels_data) < 2:
        return anomalies

    # --------------------------------------------------
    # FILTER 2: adaptive grid
    # --------------------------------------------------
    all_lats = [avg_lat for _, (_, avg_lat, _) in slow_vessels_data.items()]
    all_lons = [avg_lon for _, (_, _, avg_lon) in slow_vessels_data.items()]

    min_lat, max_lat = min(all_lats), max(all_lats)
    min_lon, max_lon = min(all_lons), max(all_lons)

    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon

    grid_size_lat = max(0.1, lat_span / 7.0)
    grid_size_lon = max(0.1, lon_span / 7.0)

    spatial_grid: Dict[tuple, List[str]] = {}

    for mmsi, (_, avg_lat, avg_lon) in slow_vessels_data.items():
        lat_cell = int((avg_lat - min_lat) / grid_size_lat)
        lon_cell = int((avg_lon - min_lon) / grid_size_lon)
        grid_cell = (lat_cell, lon_cell)

        spatial_grid.setdefault(grid_cell, []).append(mmsi)

    # --------------------------------------------------
    # FILTER 3: candidate pairs
    # --------------------------------------------------
    checked_pairs: Set[tuple] = set()
    candidate_pairs = []

    for grid_cell, mmsis_in_cell in spatial_grid.items():
        lat_cell, lon_cell = grid_cell

        adjacent_mmsis = set(mmsis_in_cell)

        for dlat in [-1, 0, 1]:
            for dlon in [-1, 0, 1]:
                neighbor_cell = (lat_cell + dlat, lon_cell + dlon)
                if neighbor_cell in spatial_grid:
                    adjacent_mmsis.update(spatial_grid[neighbor_cell])

        mmsis_list = sorted(adjacent_mmsis)

        for i, m1 in enumerate(mmsis_list):
            for m2 in mmsis_list[i + 1:]:
                pair = tuple(sorted([m1, m2]))
                if pair in checked_pairs:
                    continue

                checked_pairs.add(pair)

                rec1, avg_lat1, avg_lon1 = slow_vessels_data[m1]
                rec2, avg_lat2, avg_lon2 = slow_vessels_data[m2]

                if haversine_distance(avg_lat1, avg_lon1, avg_lat2, avg_lon2) > 50:
                    continue

                candidate_pairs.append((m1, m2, rec1, rec2))

    # --------------------------------------------------
    # FILTER 4: detailed check
    # --------------------------------------------------
    for m1, m2, rec1, rec2 in candidate_pairs:
        result = _check_loitering_pair(
            m1, m2, rec1, rec2,
            proximity_threshold_km, loitering_sec
        )
        if result:
            anomalies.append(result)

    print(f"[Loitering] Found {len(anomalies)} events")
    return anomalies


def _check_loitering_pair(
    m1, m2, rec1, rec2,
    proximity_threshold_km, loitering_sec
):
    min_epoch = max(rec1[0][1], rec2[0][1])
    max_epoch = min(rec1[-1][1], rec2[-1][1])

    if max_epoch - min_epoch < loitering_sec:
        return None

    sample1 = max(1, len(rec1) // 20)
    sample2 = max(1, len(rec2) // 20)

    proximity_count = 0
    min_dist = float('inf')
    first_ts = None
    last_ts = None

    for i in range(0, len(rec1), sample1):
        ts1, e1, lat1, lon1, _, _ = rec1[i]

        if e1 < min_epoch or e1 > max_epoch:
            continue

        for j in range(0, len(rec2), sample2):
            ts2, e2, lat2, lon2, _, _ = rec2[j]

            if abs(e1 - e2) > 900:
                continue

            dist = haversine_distance(lat1, lon1, lat2, lon2)

            if dist < proximity_threshold_km:
                proximity_count += 1
                min_dist = min(min_dist, dist)

                if first_ts is None:
                    first_ts = ts1
                last_ts = ts1

    if proximity_count >= 3:
        return {
            'mmsi_vessel1': m1,
            'mmsi_vessel2': m2,
            'anomaly_type': 'loitering',
            'loitering_start': first_ts,
            'loitering_end': last_ts,
            'duration_hours': round((max_epoch - min_epoch) / 3600, 2),
            'proximity_events': proximity_count,
            'min_distance_km': round(min_dist, 3),
        }

    return None
