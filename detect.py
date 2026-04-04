# detect.py
"""
Anomaly detection for going-dark, teleportation, and draft changes.
Runs in parallel workers for anomalies A, C, D.
"""

from typing import List, Tuple, Dict, Any
from config import (
    GOING_DARK_GAP_HOURS, GOING_DARK_MOVEMENT_KM,
    TELEPORTATION_SPEED_KNOTS, DRAFT_CHANGE_GAP_HOURS,
    DRAFT_CHANGE_PERCENT_THRESHOLD
)
from geo import haversine_distance


def detect_going_dark_anomalies(
    mmsi: str,
    records: List[Tuple],
    gap_threshold_hours: float = GOING_DARK_GAP_HOURS,
    movement_threshold_km: float = GOING_DARK_MOVEMENT_KM,
) -> List[Dict[str, Any]]:
    """
    Anomaly A: Detect AIS gaps >4 hours where vessel moved >5km (not anchored).
    """
    if len(records) < 2:
        return []

    anomalies = []
    gap_threshold_sec = gap_threshold_hours * 3600
    
    for i in range(1, len(records)):
        prev_ts_str, prev_epoch, prev_lat, prev_lon, _, _ = records[i - 1]
        curr_ts_str, curr_epoch, curr_lat, curr_lon, _, _ = records[i]

        gap_sec = curr_epoch - prev_epoch
        if gap_sec <= gap_threshold_sec:
            continue

        dist_km = haversine_distance(prev_lat, prev_lon, curr_lat, curr_lon)
        if dist_km <= movement_threshold_km:
            continue

        anomalies.append({
            'mmsi': mmsi,
            'gap_start': prev_ts_str,
            'gap_end': curr_ts_str,
            'gap_hours': round(gap_sec / 3600.0, 2),
            'distance_km': round(dist_km, 2),
            'pos_before': (prev_lat, prev_lon),
            'pos_after': (curr_lat, curr_lon),
            'anomaly_type': 'going_dark',
        })

    return anomalies


def detect_teleportation_anomalies(
    mmsi: str,
    records: List[Tuple],
    speed_threshold_knots: float = TELEPORTATION_SPEED_KNOTS,
) -> List[Dict[str, Any]]:
    """
    Anomaly D: Detect impossible vessel movements (identity cloning / teleportation).
    """
    if len(records) < 2:
        return []

    anomalies = []

    for i in range(1, len(records)):
        prev_ts_str, prev_epoch, prev_lat, prev_lon, _, _ = records[i - 1]
        curr_ts_str, curr_epoch, curr_lat, curr_lon, _, _ = records[i]

        time_sec = curr_epoch - prev_epoch
        if time_sec <= 0:
            continue
        
        dist_km = haversine_distance(prev_lat, prev_lon, curr_lat, curr_lon)
        speed_kmh = dist_km / (time_sec / 3600.0)
        speed_knots = speed_kmh / 1.852

        if speed_knots > speed_threshold_knots:
            anomalies.append({
                'mmsi': mmsi,
                'gap_start': prev_ts_str,
                'gap_end': curr_ts_str,
                'distance_km': round(dist_km, 2),
                'distance_nm': round(dist_km / 1.852, 2),
                'speed_knots': round(speed_knots, 2),
                'pos_prev': (prev_lat, prev_lon),
                'pos_curr': (curr_lat, curr_lon),
                'anomaly_type': 'teleportation',
            })

    return anomalies


def detect_draft_change_anomalies(
    mmsi: str,
    records: List[Tuple],
    gap_threshold_hours: float = DRAFT_CHANGE_GAP_HOURS,
    draft_change_percent_threshold: float = DRAFT_CHANGE_PERCENT_THRESHOLD,
) -> List[Dict[str, Any]]:
    """
    Anomaly C: Detect vessels whose draught changes >5% during AIS blackouts >2 hours.
    """
    anomalies = []
    gap_threshold_sec = gap_threshold_hours * 3600
    
    for i in range(1, len(records)):
        prev_ts_str, prev_epoch, prev_lat, prev_lon, prev_sog, prev_draft = records[i - 1]
        curr_ts_str, curr_epoch, curr_lat, curr_lon, curr_sog, curr_draft = records[i]
        
        gap_sec = curr_epoch - prev_epoch
        if gap_sec <= gap_threshold_sec:
            continue
        
        if prev_draft <= 0 or curr_draft <= 0:
            continue
        
        draft_change_percent = ((curr_draft - prev_draft) / prev_draft) * 100
        
        if abs(draft_change_percent) >= draft_change_percent_threshold:
            anomalies.append({
                'mmsi': mmsi,
                'gap_start': prev_ts_str,
                'gap_end': curr_ts_str,
                'gap_hours': round(gap_sec / 3600.0, 2),
                'draught_before': round(prev_draft, 2),
                'draught_after': round(curr_draft, 2),
                'draught_change_percent': round(draft_change_percent, 2),
                'pos_before': (prev_lat, prev_lon),
                'pos_after': (curr_lat, curr_lon),
                'anomaly_type': 'draft_change',
            })
    
    return anomalies
