#!/usr/bin/env python3
"""Comprehensive anomaly detection test"""

from utils import (
    detect_going_dark_anomalies,
    detect_teleportation_anomalies,
    detect_draft_change_anomalies,
    detect_loitering_anomalies,
    ts_to_epoch,
)

print("=" * 70)
print("COMPREHENSIVE ANOMALY DETECTION TEST")
print("=" * 70)

# Test 1: Going Dark (A)
print("\n[TEST 1] Going Dark Anomalies (A)")
print("-" * 70)
test_records_a = [
    ("01/01/2025 10:00:00", ts_to_epoch("01/01/2025 10:00:00"), 55.0, 12.0, 5.0, 8.5),
    ("01/01/2025 11:00:00", ts_to_epoch("01/01/2025 11:00:00"), 55.1, 12.1, 5.5, 8.5),
    ("01/01/2025 16:00:00", ts_to_epoch("01/01/2025 16:00:00"), 56.0, 13.0, 6.0, 8.3),  # 5hr gap
]
result_a = detect_going_dark_anomalies("test_mmsi_1", test_records_a)
print(f"Expected: 1 | Got: {len(result_a)} | {'✅ PASS' if len(result_a) == 1 else '❌ FAIL'}")

# Test 2: Teleportation (D)
print("\n[TEST 2] Teleportation Anomalies (D)")
print("-" * 70)
test_records_d = [
    ("01/01/2025 10:00:00", ts_to_epoch("01/01/2025 10:00:00"), 55.0, 12.0, 5.0, 8.5),
    ("01/01/2025 10:10:00", ts_to_epoch("01/01/2025 10:10:00"), 60.0, 20.0, 500.0, 8.5),  # Impossible
]
result_d = detect_teleportation_anomalies("test_mmsi_2", test_records_d)
print(f"Expected: 1 | Got: {len(result_d)} | {'✅ PASS' if len(result_d) == 1 else '❌ FAIL'}")
if result_d:
    print(f"  Speed: {result_d[0]['speed_knots']} knots (should be > 60)")

# Test 3: Draft Changes (C)
print("\n[TEST 3] Draft Change Anomalies (C)")
print("-" * 70)
test_records_c = [
    ("01/01/2025 10:00:00", ts_to_epoch("01/01/2025 10:00:00"), 55.0, 12.0, 0.5, 8.0),
    ("01/01/2025 13:00:00", ts_to_epoch("01/01/2025 13:00:00"), 55.1, 12.1, 0.3, 9.0),  # 12.5% change
]
result_c = detect_draft_change_anomalies("test_mmsi_3", test_records_c)
print(f"Expected: 1 | Got: {len(result_c)} | {'✅ PASS' if len(result_c) == 1 else '❌ FAIL'}")
if result_c:
    print(f"  Draft change: {result_c[0]['draught_change_percent']}% (should be ~12.5%)")

# Test 4: Loitering (B) - NEW
print("\n[TEST 4] Loitering Anomalies (B)")
print("-" * 70)
test_mmsi_records = {
    "vessel1": [
        ("01/01/2025 10:00:00", ts_to_epoch("01/01/2025 10:00:00"), 55.0, 12.0, 0.5, 8.0),
        ("01/01/2025 11:00:00", ts_to_epoch("01/01/2025 11:00:00"), 55.001, 12.001, 0.4, 8.0),
        ("01/01/2025 12:00:00", ts_to_epoch("01/01/2025 12:00:00"), 55.002, 12.002, 0.3, 8.0),
        ("01/01/2025 13:00:00", ts_to_epoch("01/01/2025 13:00:00"), 55.003, 12.003, 0.4, 8.0),
    ],
    "vessel2": [
        ("01/01/2025 10:15:00", ts_to_epoch("01/01/2025 10:15:00"), 55.0005, 12.0005, 0.5, 8.0),
        ("01/01/2025 11:15:00", ts_to_epoch("01/01/2025 11:15:00"), 55.0006, 12.0006, 0.3, 8.0),
        ("01/01/2025 12:15:00", ts_to_epoch("01/01/2025 12:15:00"), 55.0007, 12.0007, 0.4, 8.0),
        ("01/01/2025 13:15:00", ts_to_epoch("01/01/2025 13:15:00"), 55.0008, 12.0008, 0.2, 8.0),
    ]
}
result_b = detect_loitering_anomalies(test_mmsi_records)
print(f"Expected: 1 | Got: {len(result_b)} | {'✅ PASS' if len(result_b) >= 1 else '❌ FAIL'}")
if result_b:
    print(f"  Duration: {result_b[0]['duration_hours']}h (should be ~3h)")
    print(f"  Events: {result_b[0]['proximity_events']}")

print("\n" + "=" * 70)
