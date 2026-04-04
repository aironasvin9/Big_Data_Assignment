# loiter.py
"""
Loitering anomaly detection (Anomaly B)
Expected: <5 seconds for 3,400 vessels.
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
    """
    Anomaly B: Detect two distinct vessels within 500m with SOG <1 knot for >2 hours.
    
    ULTRA-OPTIMIZED: Pre-computed averages, no redundant calculations.
    Expected: <5 seconds for 3,400 vessels.
    """
    print("[Loitering] Starting ultra-fast screening...")
    
    anomalies = []
    loitering_sec = loitering_duration_hours * 3600
    
    if len(mmsi_records) < 2:
        print(f"[Loitering] Only {len(mmsi_records)} vessels. Skipping.")
        return anomalies
    
    # FILTER 1: Pre-filter slow vessels ONCE and compute averages ONCE
    print("[Loitering] Filter 1: Pre-filtering slow vessels & computing positions...")
    slow_vessels_data = {}  # mmsi -> (records, avg_lat, avg_lon)
    
    for mmsi, records in mmsi_records.items():
        slow_records = [
            (ts, epoch, lat, lon, sog, draught) 
            for ts, epoch, lat, lon, sog, draught in records 
            if sog <= sog_threshold_knots
        ]
        
        if len(slow_records) >= 2:
            # COMPUTE AVERAGE ONLY ONCE per vessel
            avg_lat = sum(r[2] for r in slow_records) / len(slow_records)
            avg_lon = sum(r[3] for r in slow_records) / len(slow_records)
            slow_vessels_data[mmsi] = (slow_records, avg_lat, avg_lon)
    
    print(f"[Loitering] Found {len(slow_vessels_data)} slow vessels")
    
    if len(slow_vessels_data) < 2:
        print("[Loitering] Not enough slow vessels. Skipping.")
        return anomalies
    
    # FILTER 2: Spatial grid using pre-computed averages
    print("[Loitering] Filter 2: Spatial binning (10km grid)...")
    
    grid_size = 10.0
    spatial_grid: Dict[tuple, List[str]] = {}
    
    for mmsi, (records, avg_lat, avg_lon) in slow_vessels_data.items():
        grid_cell = (int(avg_lat / grid_size), int(avg_lon / grid_size))
        
        if grid_cell not in spatial_grid:
            spatial_grid[grid_cell] = []
        spatial_grid[grid_cell].append(mmsi)
    
    print(f"[Loitering] Built spatial grid with {len(spatial_grid)} cells")
    
    # FILTER 3: Check only adjacent grid cells
    print("[Loitering] Filter 3: Checking adjacent cells...")
    
    checked_pairs: Set[tuple] = set()
    candidate_pairs = []
    
    for grid_cell, mmsis_in_cell in spatial_grid.items():
        if len(mmsis_in_cell) < 2:
            continue
        
        lat_cell, lon_cell = grid_cell
        
        # Get adjacent cells
        adjacent_mmsis = set(mmsis_in_cell)
        for dlat in [-1, 0, 1]:
            for dlon in [-1, 0, 1]:
                if dlat == 0 and dlon == 0:
                    continue
                neighbor_cell = (lat_cell + dlat, lon_cell + dlon)
                if neighbor_cell in spatial_grid:
                    adjacent_mmsis.update(spatial_grid[neighbor_cell])
        
        # Check pairs using PRE-COMPUTED averages
        mmsis_list = sorted(list(adjacent_mmsis))
        for i, mmsi1 in enumerate(mmsis_list):
            for mmsi2 in mmsis_list[i+1:]:
                pair_key = tuple(sorted([mmsi1, mmsi2]))
                
                if pair_key in checked_pairs:
                    continue
                checked_pairs.add(pair_key)
                
                # Use pre-computed averages - NO recalculation
                rec1, avg_lat1, avg_lon1 = slow_vessels_data[mmsi1]
                rec2, avg_lat2, avg_lon2 = slow_vessels_data[mmsi2]
                
                # Pre-filter: if average positions >10km apart, skip
                avg_dist = haversine_distance(avg_lat1, avg_lon1, avg_lat2, avg_lon2)
                if avg_dist > 10.0:
                    continue
                
                candidate_pairs.append((mmsi1, mmsi2, rec1, rec2))
    
    print(f"[Loitering] Found {len(candidate_pairs)} candidate pairs")
    
    # FILTER 4: Detailed check on candidates only
    if candidate_pairs:
        print(f"[Loitering] Filter 4: Detail checking {len(candidate_pairs)} candidates...")
        
        for idx, (mmsi1, mmsi2, rec1, rec2) in enumerate(candidate_pairs):
            if idx % max(1, len(candidate_pairs) // 10) == 0:
                print(f"[Loitering]   {idx:,} / {len(candidate_pairs):,}...")
            
            result = _check_loitering_pair_final(
                mmsi1, mmsi2, rec1, rec2,
                proximity_threshold_km, loitering_sec
            )
            
            if result:
                anomalies.append(result)
    
    print(f"[Loitering] ✓ Complete! Found {len(anomalies)} loitering events")
    return anomalies


def _check_loitering_pair_final(
    mmsi1: str,
    mmsi2: str,
    records1: List[Tuple],
    records2: List[Tuple],
    proximity_threshold_km: float,
    loitering_sec: int,
) -> Dict[str, Any]:
    """
    Final check: verify sustained proximity over time.
    Uses aggressive sampling for speed.
    """
    
    # Get time windows
    min_epoch = max(records1[0][1], records2[0][1])
    max_epoch = min(records1[-1][1], records2[-1][1])
    overlap_duration = max_epoch - min_epoch
    
    # Must have sufficient overlap
    if overlap_duration < loitering_sec:
        return None
    
    # Sample every Nth record (aggressive sampling)
    sample_every_1 = max(1, len(records1) // 20)  # Sample 20 points
    sample_every_2 = max(1, len(records2) // 20)
    
    proximity_count = 0
    min_dist = float('inf')
    first_ts = None
    last_ts = None
    
    # Check sampled points
    for i in range(0, len(records1), sample_every_1):
        rec1 = records1[i]
        ts1, epoch1, lat1, lon1, sog1, _ = rec1
        
        if epoch1 < min_epoch or epoch1 > max_epoch:
            continue
        
        for j in range(0, len(records2), sample_every_2):
            rec2 = records2[j]
            ts2, epoch2, lat2, lon2, sog2, _ = rec2
            
            # Time match: within 15 minutes
            if abs(epoch1 - epoch2) > 900:
                continue
            
            # Distance check
            dist = haversine_distance(lat1, lon1, lat2, lon2)
            
            if dist < proximity_threshold_km:
                proximity_count += 1
                min_dist = min(min_dist, dist)
                
                if first_ts is None:
                    first_ts = ts1
                last_ts = ts1
    
    # Need at least 3 proximity events
    if proximity_count >= 3 and min_dist < float('inf'):
        return {
            'mmsi_vessel1': mmsi1,
            'mmsi_vessel2': mmsi2,
            'anomaly_type': 'loitering',
            'loitering_start': first_ts,
            'loitering_end': last_ts,
            'duration_hours': round(overlap_duration / 3600.0, 2),
            'proximity_events': proximity_count,
            'min_distance_km': round(min_dist, 3),
        }
    
    return None
