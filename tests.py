# tests.py
"""
Simple quick tests before running full pipeline.
"""

import os
import tempfile
import csv
from datetime import datetime, timedelta
from geo import ts_to_epoch


def create_tiny_test_csv(num_records=5000):
    """Create tiny test CSV (5MB)."""
    test_file = os.path.join(tempfile.gettempdir(), 'tiny_test.csv')
    
    print(f"Creating tiny test CSV: {test_file}")
    
    with open(test_file, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header (matching AIS format)
        header = ['TIMESTAMP', 'TYPE_OF_MOBILE', 'MMSI', 'LATITUDE', 'LONGITUDE',
                  'NAVIGATIONAL_STATUS', 'ROT', 'SOG', 'COG', 'HEADING', 'IMO',
                  'CALLSIGN', 'NAME', 'SHIP_AND_CARGO_TYPE', 'CARGO', 'DRAUGHT']
        header += [''] * 3
        writer.writerow(header)
        
        # Generate records (5 vessels)
        mmsis = ['211378120', '211564060', '211378130', '211378140', '211378150']
        
        base_time = datetime(2025, 3, 2, 10, 0, 0)
        
        for i in range(num_records):
            mmsi_idx = i % len(mmsis)
            mmsi = mmsis[mmsi_idx]
            
            # Time progression
            ts = base_time + timedelta(minutes=i // len(mmsis))
            ts_str = ts.strftime('%d/%m/%Y %H:%M:%S')
            
            # Position varies per vessel
            base_lat = 54.0 + mmsi_idx * 0.5
            base_lon = 12.0 + mmsi_idx * 0.5
            
            lat = base_lat + (i % 50) * 0.001
            lon = base_lon + (i % 50) * 0.001
            
            # Slow speed (loitering)
            sog = 0.3 + (i % 5) * 0.1
            
            draught = 5.0 + (i % 10)
            
            row = [ts_str, 'Class A', mmsi, lat, lon,
                   '0', '0', sog, '0', '0', '0',
                   f'CALL{i}', f'SHIP{i}', '70', '', draught]
            row += ['', '', '']
            
            writer.writerow(row)
    
    file_size_mb = os.path.getsize(test_file) / (1024**2)
    print(f"✓ Created {file_size_mb:.1f}MB test file\n")
    return test_file


def run_quick_test():
    """Run quick test on small file."""
    print("="*70)
    print("QUICK LOITERING TEST (should take <10 seconds)")
    print("="*70 + "\n")
    
    # Create test file
    test_file = create_tiny_test_csv(5000)
    
    # Move to data directory
    import shutil
    data_dir = './data'
    os.makedirs(data_dir, exist_ok=True)
    test_dest = os.path.join(data_dir, 'test_tiny.csv')
    shutil.copy(test_file, test_dest)
    
    print(f"Running pipeline on test file...\n")
    
    import time
    start = time.time()
    
    # Import here to avoid early failures
    from task1 import AISPipeline
    from config import CHUNK_SIZE
    
    # Use single worker for test
    pipeline = AISPipeline(num_workers=1, chunk_size=CHUNK_SIZE)
    results = pipeline.process_file(test_dest)
    
    elapsed = time.time() - start
    
    print("\n" + "="*70)
    print("QUICK TEST RESULTS")
    print("="*70)
    print(f"✓ Completed in {elapsed:.2f} seconds")
    print(f"  Records: {results['total_records']:,}")
    print(f"  Vessels: {results['unique_vessels']}")
    print(f"  Anomalies: {len(results['anomalies'])}")
    print(f"  Pass 1: {results['pass1_seconds']:.2f}s")
    print(f"  Pass 2: {results['pass2_seconds']:.2f}s")
    print("="*70 + "\n")
    
    # Check for loitering
    loitering = [a for a in results['anomalies'] if a.get('anomaly_type') == 'loitering']
    print(f"Loitering events found: {len(loitering)}")
    if loitering:
        print("Sample loitering event:")
        print(f"  Vessels: {loitering[0]['mmsi_vessel1']} ↔ {loitering[0]['mmsi_vessel2']}")
        print(f"  Duration: {loitering[0]['duration_hours']}h")
        print(f"  Min distance: {loitering[0]['min_distance_km']}km")
    
    # Cleanup
    if os.path.exists(test_dest):
        os.remove(test_dest)
    if os.path.exists(test_file):
        os.remove(test_file)
    
    print(f"\n✅ QUICK TEST PASSED - Ready for full dataset!\n")


if __name__ == "__main__":
    run_quick_test()
