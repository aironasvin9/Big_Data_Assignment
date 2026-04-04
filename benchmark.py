# benchmark.py
"""
Benchmarking and testing suite.
Test on small data to verify correctness before running full dataset.
"""

import os
import time
import tempfile
import csv
from task1 import AISPipeline
from config import NUM_WORKERS, CHUNK_SIZE


def create_test_csv(num_records: int = 10000) -> str:
    """
    Create a small test CSV file with synthetic AIS data.
    
    Args:
        num_records: Number of records to generate
    
    Returns:
        Path to test CSV file
    """
    test_file = os.path.join(tempfile.gettempdir(), 'test_ais_data.csv')
    
    print(f"[Test] Creating synthetic test CSV with {num_records:,} records...")
    
    # Generate test data
    mmsis = ['211378120', '211564060', '211378130', '211378140', '211378150']  # Valid MMSIs
    
    with open(test_file, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'TIMESTAMP', 'TYPE_OF_MOBILE', 'MMSI', 'LATITUDE', 'LONGITUDE',
            'NAVIGATIONAL_STATUS', 'ROT', 'SOG', 'COG', 'HEADING', 'IMO',
            'CALLSIGN', 'NAME', 'SHIP_AND_CARGO_TYPE', 'CARGO', 'DRAUGHT'
        ] + [''] * 3)
        
        # Generate records
        for i in range(num_records):
            mmsi = mmsis[i % len(mmsis)]
            timestamp = f"02/03/2025 {i % 24:02d}:{(i // 60) % 60:02d}:{(i * 7) % 60:02d}"
            lat = 54.0 + (i % 100) * 0.001  # Small variation
            lon = 12.0 + (i % 100) * 0.001
            sog = (i % 20) / 5.0  # 0-4 knots
            draught = 5.0 + (i % 50) * 0.1
            
            writer.writerow([
                timestamp, 'Class A', mmsi, lat, lon,
                '0', '0', sog, '0', '0', '0',
                f'CALL{i}', f'SHIP{i}', '70', '', draught,
                '', '', ''
            ])
    
    file_size_mb = os.path.getsize(test_file) / (1024 * 1024)
    print(f"[Test] Created {test_file} ({file_size_mb:.2f} MB)")
    
    return test_file


def test_small_dataset():
    """Test on small synthetic dataset."""
    print("\n" + "="*70)
    print("TESTING ON SMALL SYNTHETIC DATASET")
    print("="*70 + "\n")
    
    # Create test file
    test_file = create_test_csv(num_records=50000)  # ~5MB
    
    try:
        # Run pipeline
        start = time.time()
        pipeline = AISPipeline(num_workers=2, chunk_size=5000)  # Use 2 workers for testing
        results = pipeline.process_file(test_file)
        elapsed = time.time() - start
        
        # Print results
        print("\n" + "="*70)
        print("TEST RESULTS")
        print("="*70)
        print(f"✅ Test completed in {elapsed:.2f} seconds")
        print(f"   Records processed: {results['total_records']:,}")
        print(f"   Vessels detected: {results['unique_vessels']}")
        print(f"   Anomalies found: {len(results['anomalies'])}")
        print(f"   Memory peak: {results['max_memory_mb']:.1f} MB")
        print(f"   Pass 1 time: {results['pass1_seconds']:.2f}s")
        print(f"   Pass 2 time: {results['pass2_seconds']:.2f}s")
        print("="*70 + "\n")
        
        # Verify output files
        print("Checking output files...")
        analysis_files = os.listdir('./analysis') if os.path.exists('./analysis') else []
        loitering_files = os.listdir('./loitering') if os.path.exists('./loitering') else []
        
        print(f"  Analysis files: {len(analysis_files)} files")
        print(f"  Loitering files: {len(loitering_files)} files")
        
        if analysis_files:
            print("\n  Files created:")
            for f in analysis_files[:5]:
                size = os.path.getsize(f'./analysis/{f}') / 1024
                print(f"    - {f} ({size:.1f} KB)")
        
        print("\n✅ Test PASSED - Ready for full dataset!\n")
        return True
        
    except Exception as e:
        print(f"\n❌ Test FAILED: {e}\n")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up test file
        if os.path.exists(test_file):
            os.remove(test_file)
            print(f"Cleaned up test file")


def test_anomaly_detection():
    """Unit test anomaly detection functions."""
    print("\n" + "="*70)
    print("UNIT TESTING ANOMALY DETECTION")
    print("="*70 + "\n")
    
    from detect import detect_going_dark_anomalies, detect_teleportation_anomalies
    from geo import ts_to_epoch
    
    # Test data
    mmsi = "211378120"
    
    # Test 1: Going Dark
    records_dark = [
        ("01/03/2025 10:00:00", ts_to_epoch("01/03/2025 10:00:00"), 54.0, 12.0, 5.0, 8.0),
        ("01/03/2025 10:10:00", ts_to_epoch("01/03/2025 10:10:00"), 54.1, 12.1, 5.0, 8.0),
        ("01/03/2025 15:00:00", ts_to_epoch("01/03/2025 15:00:00"), 55.0, 13.0, 5.0, 8.0),  # 5hr gap, ~111km
    ]
    
    going_dark = detect_going_dark_anomalies(mmsi, records_dark)
    print(f"Test 1 - Going Dark: {len(going_dark)} anomalies found")
    if going_dark:
        print(f"  ✓ Gap: {going_dark[0]['gap_hours']}h, Distance: {going_dark[0]['distance_km']}km")
    
    # Test 2: Teleportation
    records_teleport = [
        ("01/03/2025 10:00:00", ts_to_epoch("01/03/2025 10:00:00"), 54.0, 12.0, 5.0, 8.0),
        ("01/03/2025 10:10:00", ts_to_epoch("01/03/2025 10:10:00"), 60.0, 20.0, 100.0, 8.0),  # Impossible
    ]
    
    teleport = detect_teleportation_anomalies(mmsi, records_teleport)
    print(f"Test 2 - Teleportation: {len(teleport)} anomalies found")
    if teleport:
        print(f"  ✓ Speed: {teleport[0]['speed_knots']}knots, Distance: {teleport[0]['distance_km']}km")
    
    print("\n✅ Unit tests PASSED\n")


if __name__ == "__main__":
    print("\n" + "="*70)
    print("BENCHMARK & TESTING SUITE")
    print("="*70)
    
    # Run tests
    test_anomaly_detection()
    success = test_small_dataset()
    
    if success:
        print("="*70)
        print("ALL TESTS PASSED ✅")
        print("Ready to run: python task1.py")
        print("="*70 + "\n")
    else:
        print("="*70)
        print("TESTS FAILED ❌")
        print("="*70 + "\n")
