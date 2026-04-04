# analysis.py
"""
Analysis output handling - writes anomalies and summaries to CSV files.
"""

import csv
import os
import json
from typing import List, Dict, Any
from config import ANALYSIS_DIR, CSV_DELIMITER, OUTPUT_DIRS


def write_anomalies_csv(
    anomalies: List[Dict[str, Any]],
    output_file: str,
) -> None:
    """Write anomalies to CSV by type."""
    if not anomalies:
        return
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Organize by type
    by_type = {}
    for a in anomalies:
        atype = a.get('anomaly_type', 'unknown')
        if atype not in by_type:
            by_type[atype] = []
        by_type[atype].append(a)
    
    # Write each type to separate file
    for atype, items in by_type.items():
        if atype == 'going_dark':
            _write_going_dark_csv(items, os.path.join(ANALYSIS_DIR, 'going_dark_events.csv'))
        elif atype == 'teleportation':
            _write_teleportation_csv(items, os.path.join(ANALYSIS_DIR, 'teleportation_events.csv'))
        elif atype == 'draft_change':
            _write_draft_csv(items, os.path.join(ANALYSIS_DIR, 'draft_change_events.csv'))
        elif atype == 'loitering':
            _write_loitering_csv(items, os.path.join(OUTPUT_DIRS[2], 'loitering_events.csv'))


def _write_going_dark_csv(anomalies: List[Dict], filepath: str) -> None:
    """Write going-dark events."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['mmsi', 'gap_hours', 'distance_km', 'gap_start', 'gap_end'])
        writer.writeheader()
        for a in anomalies:
            writer.writerow({
                'mmsi': a['mmsi'],
                'gap_hours': a['gap_hours'],
                'distance_km': a['distance_km'],
                'gap_start': a['gap_start'],
                'gap_end': a['gap_end'],
            })
    print(f"  ✓ Wrote {len(anomalies)} going-dark events to {os.path.basename(filepath)}")


def _write_teleportation_csv(anomalies: List[Dict], filepath: str) -> None:
    """Write teleportation events."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['mmsi', 'distance_km', 'distance_nm', 'speed_knots', 'gap_start', 'gap_end'])
        writer.writeheader()
        for a in anomalies:
            writer.writerow({
                'mmsi': a['mmsi'],
                'distance_km': a['distance_km'],
                'distance_nm': a.get('distance_nm', round(a['distance_km'] / 1.852, 2)),
                'speed_knots': a['speed_knots'],
                'gap_start': a['gap_start'],
                'gap_end': a['gap_end'],
            })
    print(f"  ✓ Wrote {len(anomalies)} teleportation events to {os.path.basename(filepath)}")


def _write_draft_csv(anomalies: List[Dict], filepath: str) -> None:
    """Write draft change events."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['mmsi', 'gap_hours', 'draught_before', 'draught_after', 'draught_change_percent'])
        writer.writeheader()
        for a in anomalies:
            writer.writerow({
                'mmsi': a['mmsi'],
                'gap_hours': a['gap_hours'],
                'draught_before': a['draught_before'],
                'draught_after': a['draught_after'],
                'draught_change_percent': a['draught_change_percent'],
            })
    print(f"  ✓ Wrote {len(anomalies)} draft-change events to {os.path.basename(filepath)}")


def _write_loitering_csv(anomalies: List[Dict], filepath: str) -> None:
    """Write loitering events."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['mmsi_vessel1', 'mmsi_vessel2', 'duration_hours', 'proximity_events', 'min_distance_km'])
        writer.writeheader()
        for a in anomalies:
            writer.writerow({
                'mmsi_vessel1': a['mmsi_vessel1'],
                'mmsi_vessel2': a['mmsi_vessel2'],
                'duration_hours': a['duration_hours'],
                'proximity_events': a['proximity_events'],
                'min_distance_km': a['min_distance_km'],
            })
    print(f"  ✓ Wrote {len(anomalies)} loitering events to {os.path.basename(filepath)}")


def write_vessel_scores_csv(top_vessels: List[Dict], filepath: str) -> None:
    """Write final DFSI scores."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['rank', 'mmsi', 'dfsi', 'total_anomalies', 'anomaly_a', 'anomaly_d', 'anomaly_c', 'anomaly_b']
        )
        writer.writeheader()
        for rank, vessel in enumerate(top_vessels, 1):
            counts = vessel['anomaly_counts']
            writer.writerow({
                'rank': rank,
                'mmsi': vessel['mmsi'],
                'dfsi': vessel['dfsi'],
                'total_anomalies': vessel['total_anomalies'],
                'anomaly_a': counts.get('going_dark', 0),
                'anomaly_d': counts.get('teleportation', 0),
                'anomaly_c': counts.get('draft_change', 0),
                'anomaly_b': counts.get('loitering', 0),
            })
    print(f"  ✓ Wrote {len(top_vessels)} vessel scores to {os.path.basename(filepath)}")


def write_metadata_json(metadata: Dict, filepath: str) -> None:
    """Write run metadata."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  ✓ Wrote metadata to {os.path.basename(filepath)}")
