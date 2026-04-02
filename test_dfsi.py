#!/usr/bin/env python3
"""Test DFSI calculation"""

from utils import calculate_dfsi, aggregate_anomalies_by_vessel, rank_vessels_by_dfsi

# Simulate anomalies for a vessel
test_anomalies = [
    # Anomaly A: Going Dark (2 instances)
    {
        'mmsi': '123456789',
        'anomaly_type': 'going_dark',
        'gap_hours': 5.0,
        'distance_km': 100.0,
    },
    {
        'mmsi': '123456789',
        'anomaly_type': 'going_dark',
        'gap_hours': 8.0,
        'distance_km': 150.0,
    },
    # Anomaly D: Teleportation (2 instances)
    {
        'mmsi': '123456789',
        'anomaly_type': 'teleportation',
        'distance_km': 500.0,
        'distance_nm': 269.96,
        'speed_knots': 1500.0,
    },
    {
        'mmsi': '123456789',
        'anomaly_type': 'teleportation',
        'distance_km': 200.0,
        'distance_nm': 107.98,
        'speed_knots': 600.0,
    },
    # Anomaly C: Draft Changes (1 instance)
    {
        'mmsi': '123456789',
        'anomaly_type': 'draft_change',
        'draught_change_percent': 15.0,
    },
]

print("=" * 70)
print("DFSI CALCULATION TEST")
print("=" * 70)

# Test 1: Calculate DFSI for single vessel
print("\n[TEST 1] Manual DFSI Calculation")
print("-" * 70)
dfsi = calculate_dfsi('123456789', test_anomalies)

# Manual calculation:
# Component 1: MAX gap = 8.0 hours → 8.0 / 2 = 4.0
# Component 2: Total distance NM = 269.96 + 107.98 = 377.94 NM → 377.94 / 10 = 37.794
# Component 3: Draft changes = 1 → 1 * 15 = 15.0
# DFSI = 4.0 + 37.794 + 15.0 = 56.794

print(f"Anomalies:")
print(f"  - Going Dark: 2 instances (max gap: 8.0h)")
print(f"  - Teleportation: 2 instances (total: 377.94 NM)")
print(f"  - Draft Changes: 1 instance")
print()
print(f"Manual Calculation:")
print(f"  Component 1 (Gap): 8.0 / 2 = 4.0")
print(f"  Component 2 (Distance): 377.94 / 10 = 37.794")
print(f"  Component 3 (Draft): 1 * 15 = 15.0")
print(f"  DFSI = 4.0 + 37.794 + 15.0 = 56.794")
print()
print(f"Calculated DFSI: {dfsi}")
print(f"Expected: ~56.79")
print(f"Match: {'✅ YES' if abs(dfsi - 56.79) < 0.1 else '❌ NO'}")

# Test 2: Aggregation and ranking
print("\n[TEST 2] Vessel Aggregation & Ranking")
print("-" * 70)

# Add more vessels with different DFSI scores
extended_anomalies = test_anomalies + [
    {
        'mmsi': '987654321',
        'anomaly_type': 'going_dark',
        'gap_hours': 10.0,
        'distance_km': 200.0,
    },
    {
        'mmsi': '987654321',
        'anomaly_type': 'draft_change',
        'draught_change_percent': 20.0,
    },
    {
        'mmsi': '111111110',
        'anomaly_type': 'teleportation',
        'distance_km': 100.0,
        'distance_nm': 53.99,
        'speed_knots': 300.0,
    },
]

vessels_dict = aggregate_anomalies_by_vessel(extended_anomalies)
top_vessels = rank_vessels_by_dfsi(vessels_dict, top_n=10)

print(f"Total flagged vessels: {len(vessels_dict)}")
print(f"\nTop vessels by DFSI:")
print(f"{'Rank':<6} {'MMSI':<12} {'DFSI':>8} {'Anomalies':>10}")
print("-" * 70)
for i, vessel in enumerate(top_vessels, 1):
    print(
        f"{i:<6} {vessel['mmsi']:<12} {vessel['dfsi']:>8.2f} "
        f"{vessel['total_anomalies']:>10}"
    )

print("\n" + "=" * 70)
print("TEST COMPLETE")
print("=" * 70)
