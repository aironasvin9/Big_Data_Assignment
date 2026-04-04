"""
Loitering anomaly detection (Anomaly B) - STREAMING + GRID VERSION.

Designed for large datasets (GB-scale).
Uses:
- Sliding time window
- Spatial grid indexing
- Streaming CSV processing
"""

from typing import List, Dict, Any
from collections import defaultdict, deque

from parsing import stream_valid_rows
from geo import haversine_distance
from config import (
    LOITERING_PROXIMITY_KM,
    LOITERING_SOG_KNOTS,
    LOITERING_DURATION_HOURS
)


def detect_loitering_anomalies_streaming(filepath: str) -> List[Dict[str, Any]]:
    """
    Streaming loitering detection.

    Detects pairs of vessels:
    - Within proximity threshold
    - Moving slowly
    - Staying together for required duration

    Works without loading full dataset into memory.
    """

    print("[Loitering] Streaming detection started...")

    # --------------------------------------------------
    # CONFIG
    # --------------------------------------------------
    WINDOW_SECONDS = int(LOITERING_DURATION_HOURS * 3600 * 1.5)
    GRID_SIZE = 0.01  # ~1km grid cells

    # --------------------------------------------------
    # STATE
    # --------------------------------------------------
    vessel_tracks = defaultdict(deque)   # mmsi -> deque[(epoch, lat, lon)]
    spatial_grid = defaultdict(set)      # (cell_x, cell_y) -> set(mmsi)
    pair_start_times = {}                # (m1, m2) -> start_time

    anomalies = []

    total_rows = 0
    slow_rows = 0

    # --------------------------------------------------
    # HELPERS
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
        # FILTER 1: Slow vessels only
        # --------------------------------------------------
        if sog > LOITERING_SOG_KNOTS:
            continue

        slow_rows += 1

        # --------------------------------------------------
        # Maintain sliding window (per vessel)
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
        # Get nearby vessels (3x3 grid neighborhood)
        # --------------------------------------------------
        cx, cy = cell
        nearby_vessels = set()

        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                nearby_vessels.update(spatial_grid.get((cx + dx, cy + dy), set()))

        # --------------------------------------------------
        # Compare with nearby vessels only
        # --------------------------------------------------
        for other_mmsi in nearby_vessels:
            if other_mmsi == mmsi:
                continue

            other_track = vessel_tracks[other_mmsi]
            if not other_track:
                continue

            o_epoch, o_lat, o_lon = other_track[-1]

            # Allow small timestamp mismatch (5 minutes)
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
    print(f"  Slow vessel rows: {slow_rows:,}")
    print(f"  Unique vessels tracked: {len(vessel_tracks)}")
    print(f"  Loitering anomalies found: {len(anomalies)}")

    return anomalies
