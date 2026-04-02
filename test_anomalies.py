#!/usr/bin/env python3
"""
Quick diagnostic test for anomaly detection without multiprocessing.
Tests on a small sample to verify logic before full run.
"""

import sys
from utils import (
    detect_going_dark_anomalies,
    detect_teleportation_anomalies,
    detect_draft_change_anomalies,
    haversine_distance,
    ts_to_epoch,
)

# Test data: simulated AIS records for a single vessel
# Format: (ts_str, epoch, lat, lon, sog, draught)

test_records = [
    ("01/01/2025 10:00:00", ts_to_epoch("01/01/2025 10:00:00"), 55.0, 12.0, 5.0, 8.5),
    ("01/01/2025 11:00:00", ts_to_epoch("01/01/2025 11:00:00"), 55.1, 12.1, 5.5, 8.5),
    # 5-hour gap - should trigger Going Dark (A) if >5km movement
    ("01/01/2025 16:00:00", ts_to_epoch("01/01/2025 16:00:00"), 56.0, 13.0, 6.0, 8.3),
    ("01/01/2025 17:00:00", ts_to_epoch("01/01/2025 17:00:00"), 57.0, 14.0, 7.0, 8.2),
]

print("=" * 70)
print("ANOMALY DETECTION TEST")
print("=" * 70)

# Test 1: Going Dark (Anomaly A)
print("\n[TEST 1] Going Dark Anomalies (A)")
print("-" * 70)
result_a = detect_going_dark_anomalies("test_mmsi_1", test_records)
print(f"Expected: 1 anomaly (5hr gap, ~111km movement)")
print(f"Got: {len(result_a)} anomalies")
if result_a:
    for r in result_a:
        print(f"  - Gap: {r['gap_hours']}h, Distance: {r['distance_km']}km")
else:
    print("  ❌ NO ANOMALIES DETECTED")

# Test 2: Teleportation (Anomaly D)
print("\n[TEST 2] Teleportation Anomalies (D)")
print("-" * 70)
teleport_records = [
    ("01/01/2025 10:00:00", ts_to_epoch("01/01/2025 10:00:00"), 55.0, 12.0, 5.0, 8.5),
    ("01/01/2025 10:10:00", ts_to_epoch("01/01/2025 10:10:00"), 60.0, 20.0, 500.0, 8.5),  # Impossible speed
]
result_d = detect_teleportation_anomalies("test_mmsi_2", teleport_records)
print(f"Expected: 1 anomaly (impossible speed >60 knots)")
print(f"Got: {len(result_d)} anomalies")
if result_d:
    for r in result_d:
        print(f"  - Speed: {r['speed_knots']} knots, Distance: {r['distance_km']}km")
else:
    print("  ❌ NO ANOMALIES DETECTED")

# Test 3: Draft Changes (Anomaly C)
print("\n[TEST 3] Draft Change Anomalies (C)")
print("-" * 70)
draft_records = [
    ("01/01/2025 10:00:00", ts_to_epoch("01/01/2025 10:00:00"), 55.0, 12.0, 0.5, 8.0),
    # 3-hour gap
    ("01/01/2025 13:00:00", ts_to_epoch("01/01/2025 13:00:00"), 55.1, 12.1, 0.3, 9.0),  # 12.5% change
]
result_c = detect_draft_change_anomalies("test_mmsi_3", draft_records)
print(f"Expected: 1 anomaly (12.5% draft change during 3hr gap)")
print(f"Got: {len(result_c)} anomalies")
if result_c:
    for r in result_c:
        print(f"  - Draft change: {r['draught_change_percent']}%, Gap: {r['gap_hours']}h")
else:
    print("  ❌ NO ANOMALIES DETECTED")

print("\n" + "=" * 70)
print("TEST COMPLETE")
print("=" * 70)
