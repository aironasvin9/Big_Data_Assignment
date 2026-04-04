# benchmark.py
"""
Performance benchmarking script.
Compares Pass 1 vs Pass 2 speed separately.
"""

import os
import time
import tempfile
import csv
from datetime import datetime, timedelta
from task1 import AISPipeline
from config import CHUNK_SIZE


def create_benchmark_csv(num_records=100000, filename='bench.csv'):
    """Create larger test CSV for benchmarking."""
    test_file = os.path.join(tempfile.gettempdir(), filename)
    
    print(f"Creating benchmark CSV: {filename}")
    print(f"  Records: {num_records:,}")
    
    with open(test_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        header = [
            'TIMESTAMP', 'TYPE_OF_MOBILE', 'MMSI', 'LATITUDE', 'LONGITUDE',
            'NAVIGATIONAL_STATUS', 'ROT', 'SOG', 'COG', 'HEADING', 'IMO',
            'CALLSIGN', 'NAME', 'SHIP_AND_CARGO_TYPE', 'CARGO', 'DRAUGHT',
            '', '', ''
        ]
        writer.writerow(header)
        
        mmsis = [f'21137{8100+i}' for i in range(50)]  # 50 vessels
        base_time = datetime(2025, 3, 2, 0, 0, 0)
        
        for i in range(num_records):
            mmsi = mmsis[i % len(mmsis)]
            ts = base_time + timedelta(seconds=i % 86400)
            ts_str = ts.strftime('%d/%m/%Y %H:%M:%S')
            
            lat = 54.0 + (i % 100) * 0.01
            lon = 12.0 + (i % 100) * 0.01
            sog = (i % 200) / 10.0
            draught = 5.0 + (i % 100) / 10.0
            
            row = [ts_str, 'Class A', mmsi, lat, lon, '0', '0', sog, '0', '0', 
                   '0', f'CALL{i}', f'SHIP{i}', '70', '', draught, '', '', '']
            writer.writerow(row)
    
    size_mb = os.path.getsize(test_file) / (1024**2)
    print(f"  File size: {size_mb:.1f}MB\n")
    return test_file


def benchmark_pass1_speed():
    """Benchmark Pass 1 on different dataset sizes."""
    print("\n" + "="*70)
    print("BENCHMARK: PASS 1 SPEED (Anomalies A, C, D)")
    print("="*70 + "\n")
    
    sizes = [50000, 100000]
    
    for num_recs in sizes:
        test_file = create_benchmark_csv(num_recs, f'bench_{num_recs}.csv')
        
        data_dir = './data'
        os.makedirs(data_dir, exist_ok=True)
        test_dest = os.path.join(data_dir, f'bench_{num_recs}.csv')
        
        import shutil
        if os.path.exists(test_dest):
            os.remove(test_dest)
        shutil.copy(test_file, test_dest)
        
        print(f"Running Pass 1 benchmark on {num_recs:,} records...")
        
        start = time.time()
        pipeline = AISPipeline(num_workers=2, chunk_size=CHUNK_SIZE)
        results = pipeline.process_file(test_dest)
        elapsed = time.time() - start
        
        print(f"  ✓ Completed in {elapsed:.2f}s")
        print(f"    Pass 1: {results['pass1_seconds']:.2f}s")
        print(f"    Pass 2: {results['pass2_seconds']:.2f}s")
        print(f"    Anomalies: {len(results['anomalies'])}")
        print(f"    Memory: {results['max_memory_mb']:.1f}MB\n")
        
        if os.path.exists(test_dest):
            os.remove(test_dest)
        if os.path.exists(test_file):
            os.remove(test_file)
    
    print("="*70 + "\n")


def benchmark_pass2_speed():
    """Benchmark Pass 2 loitering detection."""
    print("\n" + "="*70)
    print("BENCHMARK: PASS 2 SPEED (Anomaly B - Loitering)")
    print("="*70 + "\n")
    
    # Use the test file from tests.py
    from tests import create_tiny_test_csv
    
    test_file = create_tiny_test_csv(50000)
    
    data_dir = './data'
    os.makedirs(data_dir, exist_ok=True)
    test_dest = os.path.join(data_dir, 'bench_pass2.csv')
    
    import shutil
    if os.path.exists(test_dest):
        os.remove(test_dest)
    shutil.copy(test_file, test_dest)
    
    print(f"Running full pipeline on loitering-heavy dataset...")
    
    start = time.time()
    pipeline = AISPipeline(num_workers=2, chunk_size=CHUNK_SIZE)
    results = pipeline.process_file(test_dest)
    elapsed = time.time() - start
    
    print(f"\n  ✓ Completed in {elapsed:.2f}s")
    print(f"    Pass 1: {results['pass1_seconds']:.2f}s (A, C, D)")
    print(f"    Pass 2: {results['pass2_seconds']:.2f}s (B) ⚡")
    print(f"    Loitering events: {len([a for a in results['anomalies'] if a.get('anomaly_type') == 'loitering'])}")
    print(f"    Memory: {results['max_memory_mb']:.1f}MB")
    print(f"    Throughput: {results['throughput_mb_per_sec']:.2f}MB/s\n")
    
    if os.path.exists(test_dest):
        os.remove(test_dest)
    if os.path.exists(test_file):
        os.remove(test_file)
    
    print("="*70 + "\n")


if __name__ == "__main__":
    print("\n" + "="*70)
    print("PERFORMANCE BENCHMARKING SUITE")
    print("="*70)
    
    benchmark_pass1_speed()
    benchmark_pass2_speed()
    
    print("✅ Benchmarking complete!\n")
