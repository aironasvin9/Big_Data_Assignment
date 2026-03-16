import os
import multiprocessing as mp
from collections import defaultdict
from typing import Dict, Any
from utilstry import stream_csv_rows, is_valid_mmsi, is_valid_coordinate, StreamingPartitioner


# CONFIGURATION & CONSTANTS

INVALID_MMSI_PATTERNS = {
    '000000000',
    '111111111',
    '222222222'
    '333333333',
    '444444444',
    '555555555',
    '666666666',
    '777777777',
    '123456789',
    '999999999',
    '012345678',
    '987654321',
    '000000001',
    '888888888',
}

INVALID_MMSI_PREFIXES = ('0000', '1111', '9999',)
EXPECTED_MMSI_LENGTH = 9
MAX_RECORDS_PER_MMSI_CHUNK = 50000
CHUNK_SIZE = 10000
NUM_WORKERS = max(1, mp.cpu_count() - 2)

# Column indices based on the AIS data structure
COL_TIMESTAMP = 0
COL_TYPE_OF_MOBILE = 1
COL_MMSI = 2
COL_LATITUDE = 3
COL_LONGITUDE = 4
COL_NAV_STATUS = 5
COL_ROT = 6
COL_SOG = 7
COL_COG = 8
COL_HEADING = 9
COL_IMO = 10
COL_CALLSIGN = 11
COL_NAME = 12
COL_SHIP_TYPE = 13
COL_DRAUGHT = 18

# STATISTICS COLLECTOR (for analysis without full processing)
# =============================================================================

def collect_file_statistics(filepath: str, sample_size: int = 100000) -> Dict[str, Any]:
    """
    Quickly collect statistics about the file without full processing.
    
    Useful for understanding data distribution before running full parallel job.
    
    Args:
        filepath: Path to CSV file
        sample_size: Number of rows to sample
        
    Returns:
        Dictionary with statistics
    """
    print(f"Collecting statistics from {os.path.basename(filepath)}...")
    print(f"Sampling first {sample_size:,} rows...")
    
    valid_rows = 0
    invalid_mmsi = 0
    invalid_coords = 0
    mmsi_sample: Dict[str, int] = defaultdict(int)
    mobile_types: Dict[str, int] = defaultdict(int)
    
    row_count = 0
    
    for row in stream_csv_rows(filepath, skip_header=True):
        row_count += 1
        if row_count == 1:  # Skip header
            continue
        if row_count > sample_size + 1:
            break
            
        if len(row) > COL_MMSI:
            mmsi = row[COL_MMSI].strip()
            
            if not is_valid_mmsi(mmsi, EXPECTED_MMSI_LENGTH, INVALID_MMSI_PATTERNS, INVALID_MMSI_PREFIXES):
                invalid_mmsi += 1
            else:
                mmsi_sample[mmsi] += 1
                valid_rows += 1
        
        if len(row) > COL_LATITUDE:
            lat = row[COL_LATITUDE] if len(row) > COL_LATITUDE else ""
            lon = row[COL_LONGITUDE] if len(row) > COL_LONGITUDE else ""
            if not is_valid_coordinate(lat, lon):
                invalid_coords += 1
        
        if len(row) > COL_TYPE_OF_MOBILE:
            mobile_types[row[COL_TYPE_OF_MOBILE]] += 1
    
    stats = {
        'sampled_rows': row_count - 1,  # Exclude header
        'valid_rows': valid_rows,
        'invalid_mmsi_count': invalid_mmsi,
        'invalid_mmsi_percent': (invalid_mmsi / (row_count - 1)) * 100 if row_count > 1 else 0,
        'invalid_coords_count': invalid_coords,
        'unique_mmsi_in_sample': len(mmsi_sample),
        'mobile_types': dict(mobile_types),
        'top_mmsi': sorted(mmsi_sample.items(), key=lambda x: x[1], reverse=True)[:10]
    }
    
    print("\nStatistics Summary:")
    print(f"  Sampled rows: {stats['sampled_rows']:,}")
    print(f"  Valid rows: {stats['valid_rows']:,}")
    print(f"  Invalid MMSI: {stats['invalid_mmsi_count']:,} ({stats['invalid_mmsi_percent']:.2f}%)")
    print(f"  Invalid coordinates: {stats['invalid_coords_count']:,}")
    print(f"  Unique vessels in sample: {stats['unique_mmsi_in_sample']:,}")
    print("\n  Mobile types distribution:")
    for mobile_type, count in sorted(stats['mobile_types'].items(), key=lambda x: x[1], reverse=True):
        print(f"    {mobile_type}: {count:,}")
    
    return stats


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point for Task 1."""
    
    # Configuration - pakeiskite i savo folderi
    DATA_DIR = "/mnt/c/Users/Namai/Desktop/VU/2nd_semester/Big_data/project/data"
    
    # Find CSV files
    csv_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.csv')])
    
    if not csv_files:
        print("No CSV files found in the data directory!")
        return
    
    print("Available AIS data files:")
    for f in csv_files:
        filepath = os.path.join(DATA_DIR, f)
        size_gb = os.path.getsize(filepath) / (1024**3)
        print(f"  - {f} ({size_gb:.2f} GB)")
    
    print("\n" + "="*70)
    print("TASK 3: SHADOW FLEET DETECTION ANALYTICS & DFSI")
    print("="*70)
    
    # Process each file
    for csv_file in csv_files:
        if not csv_file.startswith('aisdk-'):  # Only process AIS data files
            continue
            
        filepath = os.path.join(DATA_DIR, csv_file)
        
        # First, collect quick statistics
        print(f"\n{'='*70}")
        print(f"Phase 1: Quick Statistics for {csv_file}")
        print(f"{'='*70}")
        file_stats = collect_file_statistics(filepath, sample_size=100000)
        print(f"  Sample analysis complete: {file_stats['valid_rows']:,} valid rows in sample")
        
        # Then run full parallel anomaly detection
        print(f"\n{'='*70}")
        print(f"Phase 2: Full Parallel Anomaly Detection for {csv_file}")
        print(f"{'='*70}")
        
        partitioner = StreamingPartitioner(
            num_workers=NUM_WORKERS,
            chunk_size=CHUNK_SIZE
        )
        
        results = partitioner.process_file(filepath, use_mmsi_partitioning=True)
        
        print(f"\nAnomaly detection completed for {csv_file}")
        print(f"  Total valid records: {results['total_records']:,}")
        print(f"  Unique vessels: {results['unique_vessels']:,}")
        print(f"  Anomalies detected: {results['total_anomalies']:,}")
        print(f"  DFSI: {results['dfsi']}")


if __name__ == "__main__":
    # Ensure proper multiprocessing behavior on macOS
    mp.set_start_method('spawn', force=True)
    main()
