# scoring.py
"""
DFSI (Dynamic Fictional Suspicion Index) calculation and vessel ranking.
"""

from typing import List, Dict, Any
from collections import defaultdict
from config import (
    DFSI_GOING_DARK_WEIGHT, DFSI_TELEPORT_WEIGHT,
    DFSI_DRAFT_WEIGHT, DFSI_LOITERING_WEIGHT, TOP_N_VESSELS
)


def calculate_dfsi(mmsi: str, anomalies_for_vessel: List[Dict[str, Any]]) -> float:
    """Calculate Dynamic Fictional Suspicion Index (DFSI) for a vessel."""
    going_dark = [a for a in anomalies_for_vessel if a.get('anomaly_type') == 'going_dark']
    teleportation = [a for a in anomalies_for_vessel if a.get('anomaly_type') == 'teleportation']
    draft_change = [a for a in anomalies_for_vessel if a.get('anomaly_type') == 'draft_change']
    loitering = [a for a in anomalies_for_vessel if a.get('anomaly_type') == 'loitering']
    
    max_gap_hours = max((a['gap_hours'] for a in going_dark), default=0.0)
    total_distance_nm = sum(
      a.get('distance_nm') or (a.get('distance_km', 0) / 1.852)
      for a in teleportation
  )
    draft_count = len(draft_change)
    loitering_count = len(loitering)
    
    dfsi = (max_gap_hours / DFSI_GOING_DARK_WEIGHT) + \
           (total_distance_nm / DFSI_TELEPORT_WEIGHT) + \
           (draft_count * DFSI_DRAFT_WEIGHT) + \
           (loitering_count * DFSI_LOITERING_WEIGHT)
    
    return round(dfsi, 2)


def aggregate_anomalies_by_vessel(all_anomalies: List[Dict[str, Any]]) -> Dict[str, Dict]:
    """Organize anomalies by MMSI and calculate DFSI for each vessel."""
    vessels_dict: Dict[str, Dict] = defaultdict(lambda: {
        'anomalies': [],
        'anomaly_counts': defaultdict(int),
        'dfsi': 0.0
    })
    
    for anomaly in all_anomalies:
        mmsi = anomaly.get('mmsi') or anomaly.get('mmsi_vessel1')
        if not mmsi:
            continue
        
        vessels_dict[mmsi]['anomalies'].append(anomaly)
        anomaly_type = anomaly.get('anomaly_type', 'unknown')
        vessels_dict[mmsi]['anomaly_counts'][anomaly_type] += 1
    
    for mmsi, vessel_data in vessels_dict.items():
        vessel_data['dfsi'] = calculate_dfsi(mmsi, vessel_data['anomalies'])
        vessel_data['anomaly_counts'] = dict(vessel_data['anomaly_counts'])
    
    return dict(vessels_dict)


def rank_vessels_by_dfsi(vessels_dict: Dict[str, Dict], top_n: int = TOP_N_VESSELS) -> List[Dict]:
    """Rank vessels by DFSI score and return top N."""
    ranked = []
    for mmsi, data in vessels_dict.items():
        ranked.append({
            'mmsi': mmsi,
            'dfsi': data['dfsi'],
            'anomaly_counts': data['anomaly_counts'],
            'anomalies': data['anomalies'],
            'total_anomalies': len(data['anomalies']),
        })
    
    ranked.sort(key=lambda x: x['dfsi'], reverse=True)
    return ranked[:top_n]
