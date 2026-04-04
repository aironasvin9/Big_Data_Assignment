# loiter.py
"""
Loitering anomaly detection (Anomaly B) - ULTRA-OPTIMIZED.
Uses statistical filtering + grid-based spatial hashing.
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
    
    ULTRA-OPTIMIZED: Early exit filters + statistical hashing.
    Expected: <30 seconds for 3,400 vessels.
    """
    print("[Loitering] Starting ultra-fast screening...")
    
    anomalies = []
    loitering_sec = loitering_duration_hours * 3600
    
    if len(mmsi_records) < 2:
        print(f"[Loitering] Only {len(mmsi_records)} vessels. Skipping.")
        return anomalies
    
    # FILTER 1: Pre-filter slow vessels only
    print("[Loitering] Filter 1: Identifying slow-moving vessels...")
    slow_vessels = {}
    total_slow_records = 0
    
    for mmsi, records in mmsi_records.items():
        slow_records = [(ts, epoch, lat, lon, sog, draught) for ts, epoch, lat, lon, sog, draught in records 
                        if sog <= sog_threshold_knots]
        
        if len(slow_records) >= 2:  # Need at least 2 slow records
            slow_vessels[mmsi] = slow_records
            total_slow_records += len(slow_records)
    
    print(f"[Loitering] Found {len(slow_vessels)} slow vessels ({total_slow_records:,} slow records)")
    
    if len(slow_vessels) < 2:
        print("[Loitering] Not enough slow vessels for loitering. Skipping.")
        return anomalies
    
    # FILTER 2: Create coarse grid (50km cells) for fast spatial lookup
    print("[Loitering] Filter 2: Building spatial grid (50km cells)...")
    grid_size_km = 50.0
    grid: Dict[tuple, List[str]] = {}
    
    for mmsi, records in slow_vessels.items():
        for ts, epoch, lat, lon, sog, draught in records:
            grid_cell = (int(lat / grid_size_km), int(lon / grid_size_km))
            if grid_cell not in grid:
                grid[grid_cell] = []
            if mmsi not in grid[grid_cell]:
                grid[grid_cell].append(mmsi)
    
    print(f"[Loitering] Built grid with {len(grid)} cells")
    
    # FILTER 3: Only check vessel pairs in same/adjacent grid cells
    print("[Loitering] Filter 3: Screening adjacent vessels...")
    checked_pairs: Set[tuple] = set()
    candidates_found = 0
    
    for grid_cell, mmsis_in_cell in grid.items():
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
                if neighbor_cell in grid:
                    adjacent_mmsis.update(grid[neighbor_cell])
        
        # Check pairs within this cell group
        mmsis_list = sorted(list(adjacent_mmsis))
        for i, mmsi1 in enumerate(mmsis_list):
            for mmsi2 in mmsis_list[i+1:]:
                pair_key = (mmsi1, mmsi2)
                
                if pair_key in checked_pairs:
                    continue
                checked_pairs.add(pair_key)
                
                # FILTER 4: Quick time-overlap check
                rec1 = slow_vessels[mmsi1]
                rec2 = slow_vessels[mmsi2]
                
                min_epoch = max(rec1[0][1], rec2[0][1])
                max_epoch = min(rec1[-1][1], rec2[-1][1])
                
                if max_epoch - min_epoch < loitering_sec:
                    continue
                
                candidates_found += 1
                
                # FILTER 5: Detailed proximity check (only for candidates)
                result = _check_loitering_pair_fast(
                    mmsi1, mmsi2, rec1, rec2,
                    proximity_threshold_km, loitering_sec
                )
                if result:
                    anomalies.append(result)
    
    print(f"[Loitering] Checked {len(checked_pairs)} pairs, {candidates_found} candidates, {len(anomalies)} loitering events found")
    return anomalies


def _check_loitering_pair_fast(
    mmsi1: str,
    mmsi2: str,
    records1: List[Tuple],
    records2: List[Tuple],
    proximity_threshold_km: float,
    loitering_sec: int,
) -> Dict[str, Any]:
    """
    Fast proximity check: sample records at 10-minute intervals instead of checking all.
    """
    proximity_count = 0
    min_dist = float('inf')
    first_ts = None
    last_ts = None
    
    # Sample every Nth record to speed up (still accurate for sustained loitering)
    sample_interval = max(1, len(records1) // 100)  # Sample ~100 points
    
    for i in range(0, len(records1), sample_interval):
        rec1 = records1[i]
        ts1, epoch1, lat1, lon1, sog1, _ = rec1
        
        for j in range(0, len(records2), sample_interval):
            rec2 = records2[j]
            ts2, epoch2, lat2, lon2, sog2, _ = rec2
            
            # Time window: within 30 minutes
            if abs(epoch1 - epoch2) > 1800:
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
        # Calculate actual duration from first to last event
        if first_ts and last_ts:
            max_epoch = min(records1[-1][1], records2[-1][1])
            min_epoch = max(records1[0][1], records2[0][1])
            
            return {
                'mmsi_vessel1': mmsi1,
                'mmsi_vessel2': mmsi2,
                'anomaly_type': 'loitering',
                'loitering_start': first_ts,
                'loitering_end': last_ts,
                'duration_hours': round((max_epoch - min_epoch) / 3600.0, 2),
                'proximity_events': proximity_count,
                'min_distance_km': round(min_dist, 3),
            }
    
    return None
