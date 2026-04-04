"""
Loitering anomaly detection (Anomaly B)

Includes:
1. Batch version (for unit tests)
2. Streaming + grid version (for large datasets)
"""

from typing import List, Dict, Any
from collections import defaultdict, deque

from geo import haversine_distance
from parsing import stream_valid_rows
from config import (
    LOITERING_PROXIMITY_KM,
    LOITERING_SOG_KNOTS,
    LOITERING_DURATION_HOURS
)


# =====================================================================
# 🧪 BATCH VERSION (FOR UNIT TESTS)
# =====================================================================
def detect_loitering_anomalies(
    mmsi_records: Dict,
    proximity_threshold_km: float = LOITERING_PROXIMITY_KM,
    sog_threshold_knots: float = LOITERING_SOG_KNOTS,
    loitering_duration_hours: float = LOITERING_DURATION_HOURS,
) -> List[Dict[str, Any]]:
    """
    Simple batch loitering detection.

    Used only for unit tests.
    Not optimized (O(n²)), but reliable for small datasets.
    """

    anomalies = []
    duration_sec = loitering_duration_hours * 3600

    vessels = list(mmsi_records.keys())

    for i in range(len(vessels)):
        for j in range(i + 1, len(vessels)):
            m1 = vessels[i]
            m2 = vessels[j]

            recs1 = mmsi_records[m1]
            recs2 = mmsi_records[m2]

            start_time = None

            for r1 in recs1:
                for r2 in recs2:
                    ts1, e1, lat1, lon1, sog1, _ = r1
                    ts2, e2, lat2, lon2, sog2, _ = r2

                    if sog1 > sog_threshold_knots or sog2 > sog_threshold_knots:
                        continue

                    if abs(e1 - e2) > 300:
                        continue

                    dist = haversine_distance(lat1, lon1, lat2, lon2)

                    if dist <= proximity_threshold_km:
                        if start_time is None:
                            start_time = min(e1, e2)
                        else:
                            duration = max(e1, e2) - start_time

                            if duration >= duration_sec:
                                anomalies.append({
                                    'mmsi_vessel1': m1,
                                    'mmsi_vessel2': m2,
                                    'duration_hours': round(duration / 3600, 2),
                                    'min_distance_km': round(dist, 3),
                                    'anomaly_type': 'loitering',
                                })
                                start_time = None
                    else:
                        start_time = None

    return anomalies


# =====================================================================
# 🚀 STREAMING VERSION (FOR 3GB DATA)
# =====================================================================
def detect_loitering_anomalies_streaming(filepath: str) -> List[Dict[str, Any]]:
    """
    Streaming loitering detection with spatial grid.

    Designed for large datasets (GB-scale).
    """

    print("[Loitering] Streaming detection started...")

    WINDOW_SECONDS = int(LOITERING_DURATION_HOURS * 3600 * 1.5)
    GRID_SIZE = 0.01  # ~1 km grid cells

    vessel_tracks = defaultdict(deque)
    spatial_grid = defaultdict(set)
    pair_start_times = {}

    anomalies = []

    total_rows = 0
    slow_rows = 0

    # --------------------------------------------------
    # Helper: map coordinates to grid cell
    # --------------------------------------------------
    def get_cell(lat: float, lon: float):
        return (int(lat / GRID_SIZE), int(lon / GRID_SIZE))

    # --------------------------------------------------
    # STREAM PROCESSING
    # --------------------------------------------------
    for mmsi, ts_str, epoch, lat, lon, sog, draught in stream_valid_rows(filepath):

        total_rows += 1

        if total_rows % 500000 == 0:
            print(f"[Loitering] Processed {total_rows:,} rows")

        # --------------------------------------------------
        # FILTER 1: Only slow vessels
        # --------------------------------------------------
        if sog > LOITERING_SOG_KNOTS:
            continue

        slow_rows += 1

        # --------------------------------------------------
        # Maintain sliding window
        # --------------------------------------------------
        track = vessel_tracks[mmsi]

        while track and (epoch - track[0][0]) > WINDOW_SECONDS:
            old_epoch, old_lat, old_lon = track.popleft()
            old_cell = get_cell(old_lat, old_lon)
            spatial_grid[old_cell].discard(mmsi)

        track.append((epoch, lat, lon))

        # --------------------------------------------------
        # Update spatial grid
        # --------------------------------------------------
        cell = get_cell(lat, lon)
        spatial_grid[cell].add(mmsi)

        # --------------------------------------------------
        # Find nearby vessels (3x3 neighborhood)
        # --------------------------------------------------
        cx, cy = cell
        nearby_vessels = set()

        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                nearby_vessels.update(spatial_grid.get((cx + dx, cy + dy), set()))

        # --------------------------------------------------
        # Compare with nearby vessels
        # --------------------------------------------------
        for other_mmsi in nearby_vessels:
            if other_mmsi == mmsi:
                continue

            other_track = vessel_tracks[other_mmsi]
            if not other_track:
                continue

            o_epoch, o_lat, o_lon = other_track[-1]

            if abs(epoch - o_epoch) > 300:
                continue

            dist = haversine_distance(lat, lon, o_lat, o_lon)

            pair = tuple(sorted([mmsi, other_mmsi]))

            # --------------------------------------------------
            # CLOSE → track duration
            # --------------------------------------------------
            if dist <= LOITERING_PROXIMITY_KM:

                if pair not in pair_start_times:
                    pair_start_times[pair] = epoch
                else:
                    duration = epoch - pair_start_times[pair]

                    if duration >= LOITERING_DURATION_HOURS * 3600:
                        anomalies.append({
                            'mmsi_vessel1': pair[0],
                            'mmsi_vessel2': pair[1],
                            'duration_hours': round(duration / 3600, 2),
                            'min_distance_km': round(dist, 3),
                            'anomaly_type': 'loitering',
                        })

                        # reset to avoid duplicates
                        pair_start_times[pair] = epoch

            # --------------------------------------------------
            # FAR → reset tracking
            # --------------------------------------------------
            else:
                if pair in pair_start_times:
                    del pair_start_times[pair]

    # --------------------------------------------------
    # FINAL LOGS
    # --------------------------------------------------
    print("\n[Loitering] Streaming completed")
    print(f"  Total rows processed: {total_rows:,}")
    print(f"  Slow rows: {slow_rows:,}")
    print(f"  Vessels tracked: {len(vessel_tracks)}")
    print(f"  Anomalies found: {len(anomalies)}")

    return anomalies
