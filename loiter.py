# loiter.py
"""
Loitering anomaly detection (Anomaly B) - OPTIMIZED.
Detects ship-to-ship transfers.
"""

from typing import List, Tuple, Dict, Any
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
    OPTIMIZED VERSION with early termination.
    """
    anomalies = []
    loitering_sec = loitering_duration_hours * 3600
    mmsi_list = sorted(mmsi_records.keys())
    
    # Filter vessels with sufficient data
    valid_mmsis = [
        mmsi for mmsi in mmsi_list 
        if len(mmsi_records[mmsi]) >= 2
    ]
    
    if len(valid_mmsis) < 2:
        print(f"[Loitering] Only {len(valid_mmsis)} vessels. Skipping.")
        return anomalies
    
    total_pairs = len(valid_mmsis) * (len(valid_mmsis) - 1) // 2
    print(f"[Loitering] Checking {total_pairs:,} vessel pairs...")
    
    pairs_checked = 0
    
    for idx1 in range(len(valid_mmsis)):
        for idx2 in range(idx1 + 1, len(valid_mmsis)):
            mmsi1 = valid_mmsis[idx1]
            mmsi2 = valid_mmsis[idx2]
            
            records1 = mmsi_records[mmsi1]
            records2 = mmsi_records[mmsi2]
            
            pairs_checked += 1
            if pairs_checked % 10000 == 0:
                print(f"[Loitering] {pairs_checked:,} / {total_pairs:,} pairs...")
            
            # Early exit: no time overlap
            min_epoch = max(records1[0][1], records2[0][1])
            max_epoch = min(records1[-1][1], records2[-1][1])
            
            if max_epoch - min_epoch < loitering_sec:
                continue
            
            # Pre-filter slow-moving records
            slow_records1 = [
                rec for rec in records1 
                if rec[4] <= sog_threshold_knots
            ]
            slow_records2 = [
                rec for rec in records2 
                if rec[4] <= sog_threshold_knots
            ]
            
            if len(slow_records1) < 2 or len(slow_records2) < 2:
                continue
            
            # Find proximity windows
            proximity_windows = []
            
            for rec1 in slow_records1:
                ts1, epoch1, lat1, lon1, sog1, _ = rec1
                
                for rec2 in slow_records2:
                    ts2, epoch2, lat2, lon2, sog2, _ = rec2
                    
                    if abs(epoch1 - epoch2) > 900:
                        continue
                    
                    dist_km = haversine_distance(lat1, lon1, lat2, lon2)
                    
                    if dist_km < proximity_threshold_km:
                        proximity_windows.append({
                            'epoch': epoch1,
                            'ts1': ts1,
                            'dist_km': dist_km,
                            'sog1': sog1,
                            'sog2': sog2,
                        })
            
            if len(proximity_windows) < 3:
                continue
            
            proximity_windows.sort(key=lambda x: x['epoch'])
            duration = proximity_windows[-1]['epoch'] - proximity_windows[0]['epoch']
            
            if duration >= loitering_sec:
                anomalies.append({
                    'mmsi_vessel1': mmsi1,
                    'mmsi_vessel2': mmsi2,
                    'anomaly_type': 'loitering',
                    'loitering_start': proximity_windows[0]['ts1'],
                    'loitering_end': proximity_windows[-1]['ts1'],
                    'duration_hours': round(duration / 3600.0, 2),
                    'proximity_events': len(proximity_windows),
                    'min_distance_km': round(min(p['dist_km'] for p in proximity_windows), 3),
                })
    
    print(f"[Loitering] Found {len(anomalies)} loitering events")
    return anomalies
