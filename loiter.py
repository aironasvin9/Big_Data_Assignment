# loiter.py
"""
Loitering anomaly detection (Anomaly B)
Intelligent multi-stage filtering to handle large files efficiently.
"""

import gc
from typing import List, Tuple, Dict, Any, Set
from collections import defaultdict
from config import (
    LOITERING_PROXIMITY_KM, LOITERING_SOG_KNOTS,
    LOITERING_DURATION_HOURS
)
from geo import haversine_distance
from parsing import stream_valid_rows


def detect_loitering_anomalies(
    mmsi_records: Dict[str, List[Tuple]],
    proximity_threshold_km: float = LOITERING_PROXIMITY_KM,
    sog_threshold_knots: float = LOITERING_SOG_KNOTS,
    loitering_duration_hours: float = LOITERING_DURATION_HOURS,
) -> List[Dict[str, Any]]:
    """
    Anomaly B: Detect two distinct vessels within 500m with SOG <1 knot for >2 hours.
    Uses adaptive spatial grid sizing based on geographic spread.
    """
    print("[Loitering] Starting ultra-fast screening...")
    
    anomalies = []
    loitering_sec = loitering_duration_hours * 3600
    
    if len(mmsi_records) < 2:
        print(f"[Loitering] Only {len(mmsi_records)} vessels. Skipping.")
        return anomalies
    
    # FILTER 1: Pre-filter slow vessels & compute positions
    print("[Loitering] Filter 1: Pre-filtering slow vessels...")
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
    
    print(f"[Loitering] Found {len(slow_vessels_data)} slow vessels")
    
    if len(slow_vessels_data) < 2:
        print("[Loitering] Not enough slow vessels. Skipping.")
        return anomalies
    
    # FILTER 2: Adaptive spatial grid
    print("[Loitering] Filter 2: Building spatial grid...")
    
    # Find geographic bounds
    all_lats = [avg_lat for _, (_, avg_lat, _) in slow_vessels_data.items()]
    all_lons = [avg_lon for _, (_, _, avg_lon) in slow_vessels_data.items()]
    
    min_lat, max_lat = min(all_lats), max(all_lats)
    min_lon, max_lon = min(all_lons), max(all_lons)
    
    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon
    
    # Adaptive grid size: aim for ~100-200 cells total
    num_cells = max(10, min(200, len(slow_vessels_data) // 5))
    grid_rows = int(num_cells ** 0.5)
    grid_cols = int(num_cells / grid_rows) + 1
    
    grid_size_lat = max(0.01, lat_span / grid_rows) if lat_span > 0 else 1.0
    grid_size_lon = max(0.01, lon_span / grid_cols) if lon_span > 0 else 1.0
    
    print(f"[Loitering] Grid: {grid_rows}x{grid_cols} = {grid_rows*grid_cols} cells")
    print(f"[Loitering] Cell sizes: {grid_size_lat:.3f}° × {grid_size_lon:.3f}°")
    
    spatial_grid: Dict[tuple, List[str]] = {}
    
    for mmsi, (records, avg_lat, avg_lon) in slow_vessels_data.items():
        lat_cell = int((avg_lat - min_lat) / grid_size_lat) if grid_size_lat > 0 else 0
        lon_cell = int((avg_lon - min_lon) / grid_size_lon) if grid_size_lon > 0 else 0
        grid_cell = (lat_cell, lon_cell)
        
        if grid_cell not in spatial_grid:
            spatial_grid[grid_cell] = []
        spatial_grid[grid_cell].append(mmsi)
    
    print(f"[Loitering] Built spatial grid with {len(spatial_grid)} active cells")
    
    # FILTER 3: Pre-compute distance matrix for grid cells
    print("[Loitering] Filter 3: Computing cell-to-cell distances...")
    cell_centers = {}
    for cell, mmsis in spatial_grid.items():
        lat_cell, lon_cell = cell
        center_lat = min_lat + (lat_cell + 0.5) * grid_size_lat
        center_lon = min_lon + (lon_cell + 0.5) * grid_size_lon
        cell_centers[cell] = (center_lat, center_lon)
    
    # Find which cells are close to each other
    close_cell_pairs: Set[Tuple[tuple, tuple]] = set()
    cell_list = list(spatial_grid.keys())
    for i, cell1 in enumerate(cell_list):
        lat1, lon1 = cell_centers[cell1]
        for cell2 in cell_list[i+1:]:
            lat2, lon2 = cell_centers[cell2]
            dist = haversine_distance(lat1, lon1, lat2, lon2)
            
            # Only keep cell pairs within 50km (will refine later)
            if dist <= 50.0:
                close_cell_pairs.add((min(cell1, cell2), max(cell1, cell2)))
    
    print(f"[Loitering] Found {len(close_cell_pairs)} close cell pairs")
    
    # FILTER 4: Check only vessel pairs in close cells
    print("[Loitering] Filter 4: Screening vessel pairs in close cells...")
    
    checked_pairs: Set[tuple] = set()
    candidate_pairs = []
    
    for cell1, cell2 in close_cell_pairs:
        mmsis1 = spatial_grid[cell1]
        mmsis2 = spatial_grid[cell2]
        
        # Check all pairs between these two cells
        for mmsi1 in mmsis1:
            for mmsi2 in mmsis2:
                if mmsi1 >= mmsi2:  # Avoid duplicates
                    continue
                
                pair_key = (mmsi1, mmsi2)
                if pair_key in checked_pairs:
                    continue
                checked_pairs.add(pair_key)
                
                rec1, avg_lat1, avg_lon1 = slow_vessels_data[mmsi1]
                rec2, avg_lat2, avg_lon2 = slow_vessels_data[mmsi2]
                
                # Distance pre-filter: 50km threshold
                avg_dist = haversine_distance(avg_lat1, avg_lon1, avg_lat2, avg_lon2)
                if avg_dist > 50.0:
                    continue
                
                candidate_pairs.append((mmsi1, mmsi2, rec1, rec2))
    
    print(f"[Loitering] Found {len(candidate_pairs)} candidate pairs (from {len(checked_pairs)} checked)")
    
    # FILTER 5: Detailed check on candidates
    if candidate_pairs:
        print(f"[Loitering] Filter 5: Detail checking {len(candidate_pairs)} candidates...")
        
        for idx, (mmsi1, mmsi2, rec1, rec2) in enumerate(candidate_pairs):
            if idx % max(1, len(candidate_pairs) // 10) == 0 and idx > 0:
                print(f"[Loitering]   {idx:,} / {len(candidate_pairs):,}...")
            
            result = _check_loitering_pair_final(
                mmsi1, mmsi2, rec1, rec2,
                proximity_threshold_km, loitering_sec
            )
            
            if result:
                anomalies.append(result)
    
    print(f"[Loitering] ✓ Complete! Found {len(anomalies)} loitering events")
    return anomalies


def detect_loitering_anomalies_streaming(
    filepath: str,
    proximity_threshold_km: float = LOITERING_PROXIMITY_KM,
    sog_threshold_knots: float = LOITERING_SOG_KNOTS,
    loitering_duration_hours: float = LOITERING_DURATION_HOURS,
) -> List[Dict[str, Any]]:
    """
    Loitering detection directly from CSV file - STREAMING with aggressive filtering.
    Memory usage: ~200MB max (streaming + filtered candidates only)
    """
    print("[Loitering] Starting streaming detection (aggressive filtering mode)...")
    
    anomalies = []
    loitering_sec = loitering_duration_hours * 3600
    
    # PHASE 1: Stream file and collect slow vessels ONLY
    print("[Loitering] Phase 1: Streaming slow vessels from CSV...")
    slow_vessels_data = defaultdict(list)
    total_scanned = 0
    
    for mmsi, ts_str, epoch, lat, lon, sog, draught in stream_valid_rows(filepath):
        if sog <= sog_threshold_knots:
            slow_vessels_data[mmsi].append((ts_str, epoch, lat, lon, sog, draught))
        
        total_scanned += 1
        if total_scanned % 2000000 == 0:
            print(f"[Loitering]   Scanned {total_scanned:,} records, "
                  f"slow vessels: {len(slow_vessels_data)}")
            gc.collect()
    
    print(f"[Loitering] ✓ Scanned {total_scanned:,} total records")
    print(f"[Loitering] Found {len(slow_vessels_data)} slow vessels")
    
    if len(slow_vessels_data) < 2:
        print("[Loitering] Not enough slow vessels for loitering. Skipping.")
        return anomalies
    
    # PHASE 2: Compute positions
    print("[Loitering] Phase 2: Computing vessel positions...")
    vessel_positions = {}
    for mmsi, records in slow_vessels_data.items():
        avg_lat = sum(r[2] for r in records) / len(records)
        avg_lon = sum(r[3] for r in records) / len(records)
        vessel_positions[mmsi] = (avg_lat, avg_lon)
    
    # PHASE 3: Build spatial grid with FINER granularity
    print("[Loitering] Phase 3: Building adaptive spatial grid...")
    
    all_lats = [lat for lat, lon in vessel_positions.values()]
    all_lons = [lon for lat, lon in vessel_positions.values()]
    
    min_lat, max_lat = min(all_lats), max(all_lats)
    min_lon, max_lon = min(all_lons), max(all_lons)
    
    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon
    
    # Create finer grid: aim for ~5-10 vessels per cell
    num_cells = max(50, min(500, len(vessel_positions) // 8))
    grid_rows = int(num_cells ** 0.5)
    grid_cols = int(num_cells / grid_rows) + 1
    
    grid_size_lat = max(0.01, lat_span / grid_rows) if lat_span > 0 else 1.0
    grid_size_lon = max(0.01, lon_span / grid_cols) if lon_span > 0 else 1.0
    
    print(f"[Loitering] Grid: {grid_rows}x{grid_cols} = {grid_rows*grid_cols} cells")
    
    spatial_grid = defaultdict(list)
    for mmsi, (lat, lon) in vessel_positions.items():
        lat_cell = int((lat - min_lat) / grid_size_lat) if grid_size_lat > 0 else 0
        lon_cell = int((lon - min_lon) / grid_size_lon) if grid_size_lon > 0 else 0
        spatial_grid[(lat_cell, lon_cell)].append(mmsi)
    
    print(f"[Loitering] Built grid with {len(spatial_grid)} active cells")
    cell_sizes = [len(mmsis) for mmsis in spatial_grid.values()]
    print(f"[Loitering] Cell sizes: min={min(cell_sizes)}, avg={sum(cell_sizes)//len(cell_sizes)}, max={max(cell_sizes)}")
    
    # PHASE 4: Find candidate pairs ONLY from close cells
    print("[Loitering] Phase 4: Finding candidate pairs from close cells...")
    
    # Pre-compute cell centers
    cell_centers = {}
    for cell in spatial_grid.keys():
        lat_cell, lon_cell = cell
        center_lat = min_lat + (lat_cell + 0.5) * grid_size_lat
        center_lon = min_lon + (lon_cell + 0.5) * grid_size_lon
        cell_centers[cell] = (center_lat, center_lon)
    
    # Find close cell pairs
    close_cell_pairs: Set[Tuple[tuple, tuple]] = set()
    cell_list = list(spatial_grid.keys())
    for i, cell1 in enumerate(cell_list):
        lat1, lon1 = cell_centers[cell1]
        for cell2 in cell_list[i+1:]:
            lat2, lon2 = cell_centers[cell2]
            dist = haversine_distance(lat1, lon1, lat2, lon2)
            
            if dist <= 50.0:  # Pre-filter
                close_cell_pairs.add((min(cell1, cell2), max(cell1, cell2)))
    
    print(f"[Loitering] Found {len(close_cell_pairs)} close cell pairs")
    
    # Collect candidate vessel pairs
    checked_pairs = set()
    candidate_pairs = []
    
    for cell1, cell2 in close_cell_pairs:
        mmsis1 = spatial_grid[cell1]
        mmsis2 = spatial_grid[cell2]
        
        for mmsi1 in mmsis1:
            for mmsi2 in mmsis2:
                if mmsi1 >= mmsi2:
                    continue
                
                pair_key = (mmsi1, mmsi2)
                if pair_key in checked_pairs:
                    continue
                checked_pairs.add(pair_key)
                
                lat1, lon1 = vessel_positions[mmsi1]
                lat2, lon2 = vessel_positions[mmsi2]
                
                avg_dist = haversine_distance(lat1, lon1, lat2, lon2)
                if avg_dist > 50.0:
                    continue
                
                rec1 = slow_vessels_data[mmsi1]
                rec2 = slow_vessels_data[mmsi2]
                candidate_pairs.append((mmsi1, mmsi2, rec1, rec2))
    
    print(f"[Loitering] Found {len(candidate_pairs)} candidate pairs to check")
    
    # PHASE 5: Detail check
    print(f"[Loitering] Phase 5: Detail checking {len(candidate_pairs)} candidates...")
    for idx, (mmsi1, mmsi2, rec1, rec2) in enumerate(candidate_pairs):
        if idx % max(1, len(candidate_pairs) // 10) == 0 and idx > 0:
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
    """Final proximity check with aggressive sampling."""
    
    min_epoch = max(records1[0][1], records2[0][1])
    max_epoch = min(records1[-1][1], records2[-1][1])
    overlap_duration = max_epoch - min_epoch
    
    if overlap_duration < loitering_sec:
        return None
    
    # Aggressive sampling: only check 10 points per vessel
    sample_every_1 = max(1, len(records1) // 10)
    sample_every_2 = max(1, len(records2) // 10)
    
    proximity_count = 0
    min_dist = float('inf')
    first_ts = None
    last_ts = None
    
    for i in range(0, len(records1), sample_every_1):
        rec1 = records1[i]
        ts1, epoch1, lat1, lon1, sog1, _ = rec1
        
        if epoch1 < min_epoch or epoch1 > max_epoch:
            continue
        
        for j in range(0, len(records2), sample_every_2):
            rec2 = records2[j]
            ts2, epoch2, lat2, lon2, sog2, _ = rec2
            
            # Time match: within 30 minutes
            if abs(epoch1 - epoch2) > 1800:
                continue
            
            dist = haversine_distance(lat1, lon1, lat2, lon2)
            
            if dist < proximity_threshold_km:
                proximity_count += 1
                min_dist = min(min_dist, dist)
                
                if first_ts is None:
                    first_ts = ts1
                last_ts = ts1
    
    # Need at least 3 proximity events to flag
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
