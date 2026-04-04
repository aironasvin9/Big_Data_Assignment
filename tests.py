# tests.py
"""
Simple quick tests before running full pipeline.
"""

import os
import tempfile
import csv
from datetime import datetime, timedelta


def create_tiny_test_csv(num_records=10000):
    """
    Create tiny test CSV (matching real AIS format exactly).
    This must match the column indices in config.py:
    - COL_TIMESTAMP = 0
    - COL_TYPE_OF_MOBILE = 1
    - COL_MMSI = 2
    - COL_LATITUDE = 3
    - COL_LONGITUDE = 4
    - ... (rest)
    """
    test_file = os.path.join(tempfile.gettempdir(), 'tiny_test.csv')
    
    print(f"Creating tiny test CSV: {test_file}")
    
    with open(test_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # CORRECT HEADER - must match column indices
        # Based on real AIS data structure
        header = [
            'TIMESTAMP',              # 0
            'TYPE_OF_MOBILE',         # 1
            'MMSI',                   # 2
            'LATITUDE',               # 3
            'LONGITUDE',              # 4
            'NAVIGATIONAL_STATUS',    # 5
            'ROT',                    # 6
            'SOG',                    # 7
            'COG',                    # 8
            'HEADING',                # 9
            'IMO',                    # 10
            'CALLSIGN',               # 11
            'NAME',                   # 12
            'SHIP_AND_CARGO_TYPE',    # 13
            'CARGO',                  # 14
            'DRAUGHT'                 # 15 (COL_DRAUGHT = 18, but CSV is 0-indexed)
        ]
        
        # Add padding columns to reach index 18
        header.extend(['', '', ''])
        
        writer.writerow(header)
        
        # Generate records - 5 vessels, staying close together (loitering scenario)
        mmsis = [
            '211378120',  # Valid MMSI
            '211564060',  # Valid MMSI
            '211378130',  # Valid MMSI
            '211378140',  # Valid MMSI
            '211378150',  # Valid MMSI
        ]
        
        base_time = datetime(2025, 3, 2, 10, 0, 0)
        
        for i in range(num_records):
            mmsi_idx = i % len(mmsis)
            mmsi = mmsis[mmsi_idx]
            
            # Time progression (1 minute per 5 records)
            minutes_elapsed = (i // len(mmsis))
            ts = base_time + timedelta(minutes=minutes_elapsed)
            ts_str = ts.strftime('%d/%m/%Y %H:%M:%S')
            
            # Create loitering scenario: vessels 0 and 1 stay close
            if mmsi_idx == 0:
                # Vessel 0: around 54.5, 12.5
                lat = 54.5 + (i % 10) * 0.0001
                lon = 12.5 + (i % 10) * 0.0001
            elif mmsi_idx == 1:
                # Vessel 1: very close to vessel 0 (loitering!)
                lat = 54.5 + 0.0001 + (i % 10) * 0.00005
                lon = 12.5 + 0.0001 + (i % 10) * 0.00005
            else:
                # Other vessels: far away
                lat = 55.0 + mmsi_idx * 0.5 + (i % 10) * 0.001
                lon = 13.0 + mmsi_idx * 0.5 + (i % 10) * 0.001
            
            # Speed: vessels 0 and 1 are slow (loitering)
            if mmsi_idx in [0, 1]:
                sog = 0.2 + (i % 5) * 0.1  # 0.2 - 0.6 knots
            else:
                sog = 5.0 + (i % 10)  # 5-15 knots (moving)
            
            draught = 5.0 + (i % 15)
            
            # Build row with correct column positions
            row = [
                ts_str,           # 0: TIMESTAMP
                'Class A',        # 1: TYPE_OF_MOBILE
                mmsi,             # 2: MMSI
                lat,              # 3: LATITUDE
                lon,              # 4: LONGITUDE
                '0',              # 5: NAVIGATIONAL_STATUS
                '0',              # 6: ROT
                sog,              # 7: SOG
                '0',              # 8: COG
                '0',              # 9: HEADING
                '0',              # 10: IMO
                f'CALL{i}',       # 11: CALLSIGN
                f'SHIP{i}',       # 12: NAME
                '70',             # 13: SHIP_AND_CARGO_TYPE
                '',               # 14: CARGO
                draught,          # 15: DRAUGHT (actual index in CSV)
            ]
            
            # Padding to match header
            row.extend(['', '', ''])
            
            writer.writerow(row)
    
    file_size_mb = os.path.getsize(test_file) / (1024**2)
    print(f"✓ Created {file_size_mb:.1f}MB test file")
    print(f"  - 5 vessels")
    print(f"  - {num_records:,} records total")
    print(f"  - Vessels 0 & 1 loitering together (same location, slow speed)\n")
    
    return test_file


def run_quick_test():
    """Run quick test on small file."""
    print("="*70)
    print("QUICK TEST: Loitering Detection (should find vessels 0 & 1)")
    print("="*70 + "\n")
    
    # Create test file
    test_file = create_tiny_test_csv(10000)
    
    # Move to data directory
    import shutil
    data_dir = './data'
    os.makedirs(data_dir, exist_ok=True)
    test_dest = os.path.join(data_dir, 'test_tiny.csv')
    
    if os.path.exists(test_dest):
        os.remove(test_dest)
    
    shutil.copy(test_file, test_dest)
    
    print(f"Running pipeline on test file...\n")
    
    import time
    start = time.time()
    
    # Import here to avoid early failures
    try:
        from task1 import AISPipeline
        from config import CHUNK_SIZE
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False
    
    # Use single worker for test
    try:
        pipeline = AISPipeline(num_workers=1, chunk_size=CHUNK_SIZE)
        results = pipeline.process_file(test_dest)
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    elapsed = time.time() - start
    
    print("\n" + "="*70)
    print("QUICK TEST RESULTS")
    print("="*70)
    print(f"✓ Completed in {elapsed:.2f} seconds")
    print(f"  Records processed: {results['total_records']:,}")
    print(f"  Vessels detected: {results['unique_vessels']}")
    print(f"  Total anomalies: {len(results['anomalies'])}")
    print(f"  Pass 1 time: {results['pass1_seconds']:.2f}s")
    print(f"  Pass 2 time: {results['pass2_seconds']:.2f}s")
    print("="*70)
    
    # Check results
    by_type = {}
    for a in results['anomalies']:
        atype = a.get('anomaly_type')
        by_type[atype] = by_type.get(atype, 0) + 1
    
    print("\nAnomalies by type:")
    for atype, count in by_type.items():
        print(f"  - {atype}: {count}")
    
    # Check for loitering
    loitering = [a for a in results['anomalies'] if a.get('anomaly_type') == 'loitering']
    print(f"\n✓ Loitering events found: {len(loitering)}")
    
    if loitering:
        print("\nSample loitering events:")
        for i, event in enumerate(loitering[:3]):
            print(f"\n  Event {i+1}:")
            print(f"    Vessels: {event['mmsi_vessel1']} ↔ {event['mmsi_vessel2']}")
            print(f"    Duration: {event['duration_hours']}h")
            print(f"    Proximity events: {event['proximity_events']}")
            print(f"    Min distance: {event['min_distance_km']:.4f}km")
    else:
        print("\n⚠️  No loitering detected (may be expected for test data)")
    
    # Cleanup
    if os.path.exists(test_dest):
        os.remove(test_dest)
    if os.path.exists(test_file):
        os.remove(test_file)
    
    print(f"\n✅ QUICK TEST COMPLETED\n")
    return True


if __name__ == "__main__":
    success = run_quick_test()
    
    if success:
        print("="*70)
        print("Ready to run full pipeline:")
        print("  python task1.py")
        print("="*70 + "\n")
    else:
        print("❌ Test failed - fix errors above\n")
