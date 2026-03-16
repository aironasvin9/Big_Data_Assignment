import resource
import csv
from typing import Generator, List, Tuple, Dict, Any
import gc
from collections import defaultdict
from multiprocessing import Process, Queue, Value
from multiprocessing.sharedctypes import Synchronized
import os
import time
import multiprocessing as mp
import datetime
from math import radians, sin, cos, sqrt, atan2

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


# UTILITY FUNCTIONS FOR ANOMALY DETECTION

def parse_timestamp(ts_str: str) -> datetime.datetime:
    """Parse timestamp in format DD/MM/YYYY HH:MM:SS"""
    return datetime.datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate haversine distance in kilometers"""
    R = 6371  # Earth radius in km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

# FIX 1: Sort rows before passing to detect_going_dark_anomalies,
# so the detection function can assume chronological order and skip re-sorting.
def detect_going_dark_anomalies(rows: List[List[str]]) -> List[Dict[str, Any]]:
    """
    Detect 'Going Dark' anomalies for a vessel's chronological records.
    Rows must already be sorted by timestamp before calling this function.
    """
    if len(rows) < 2:
        return []

    anomalies = []
    for i in range(1, len(rows)):
        prev_row = rows[i-1]
        curr_row = rows[i]
        
        prev_ts = parse_timestamp(prev_row[COL_TIMESTAMP])
        curr_ts = parse_timestamp(curr_row[COL_TIMESTAMP])
        
        gap_hours = (curr_ts - prev_ts).total_seconds() / 3600
        
        if gap_hours > 4:
            try:
                prev_lat = float(prev_row[COL_LATITUDE])
                prev_lon = float(prev_row[COL_LONGITUDE])
                curr_lat = float(curr_row[COL_LATITUDE])
                curr_lon = float(curr_row[COL_LONGITUDE])
                
                dist = haversine_distance(prev_lat, prev_lon, curr_lat, curr_lon)
                
                if dist > 0.1:
                    anomalies.append({
                        'mmsi': prev_row[COL_MMSI],
                        'gap_start': prev_row[COL_TIMESTAMP],
                        'gap_end': curr_row[COL_TIMESTAMP],
                        'gap_hours': gap_hours,
                        'distance_km': dist,
                        'prev_pos': (prev_lat, prev_lon),
                        'curr_pos': (curr_lat, curr_lon),
                        'anomaly_type': 'going_dark'
                    })
            except (ValueError, IndexError):
                continue
    
    return anomalies



# MEMORY MONITORING UTILITIES

def get_memory_usage_mb():
    """Get current memory usage in MB (macOS/Linux)."""
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_maxrss / (1024 * 1024)  # Convert to MB on macOS
    except Exception:
        return 0.0


def check_memory_limit(limit_mb = 1000.0):
    """Check if memory usage is under the limit (default 1GB)."""
    current = get_memory_usage_mb()
    return current < limit_mb

# DATA VALIDATION & FILTERING
def is_valid_mmsi(mmsi, EXPECTED_MMSI_LENGTH, INVALID_MMSI_PATTERNS, INVALID_MMSI_PREFIXES):
    """
    Check whether MMSI code is valid according to standard rules.
    """

    mmsi = mmsi.strip()
    
    if not mmsi:
        return False
    
    if not mmsi.isdigit():
        return False
    
    if len(mmsi) != EXPECTED_MMSI_LENGTH:
        return False
    
    if mmsi in INVALID_MMSI_PATTERNS:
        return False
    
    if mmsi.startswith(INVALID_MMSI_PREFIXES):
        return False
    
    if len(set(mmsi)) == 1:
        return False
    
    return True

def is_valid_coordinate(lat, lon):
    """
    Validate latitude and longitude values.
    """
    try:
        lat_f = float(lat)
        lon_f = float(lon)
        
        if not (-90 <= lat_f <= 90):
            return False
        if not (-180 <= lon_f <= 180):
            return False
        
        if lat_f == 0.0 and lon_f == 0.0:
            return False
            
        return True
    except (ValueError, TypeError):
        return False
    
def validate_row(row, COL_MMSI, COL_LATITUDE, COL_LONGITUDE):
    """
    Validate a complete row of AIS data.
    """
    
    mmsi = row[COL_MMSI] if len(row) > COL_MMSI else ""
    if not is_valid_mmsi(mmsi, EXPECTED_MMSI_LENGTH, INVALID_MMSI_PATTERNS, INVALID_MMSI_PREFIXES):
        return False
    
    lat = row[COL_LATITUDE] if len(row) > COL_LATITUDE else ""
    lon = row[COL_LONGITUDE] if len(row) > COL_LONGITUDE else ""
    if not is_valid_coordinate(lat, lon):
        return False
    
    return True


# STREAMING DATA GENERATOR

def stream_csv_rows(filepath, skip_header = True) -> Generator[List[str], None, None]:
    """
    Generator that streams CSV rows one at a time without loading entire file.
    """
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        
        if skip_header:
            try:
                header = next(reader)
                yield header
            except StopIteration:
                return
        
        for row in reader:
            yield row


def stream_valid_rows(filepath, COL_MMSI) -> Generator[Tuple[str, List[str]], None, None]:
    """
    Generator that streams only valid rows, yielding (MMSI, row) tuples.
    """
    row_generator = stream_csv_rows(filepath, skip_header=True)

    try:
        _header = next(row_generator)
    except StopIteration:
        return
    
    for row in row_generator:
        is_valid = validate_row(row, COL_MMSI, COL_LATITUDE, COL_LONGITUDE)
        if is_valid:
            mmsi = row[COL_MMSI].strip()
            yield (mmsi, row)


# CHUNK PARTITIONING
def create_chunks(filepath, chunk_size) -> Generator[List[Tuple[str, List[str]]], None, None]:
    """
    Generator that creates chunks of valid rows for parallel processing.
    """
    chunk = []
    
    for mmsi, row in stream_valid_rows(filepath, COL_MMSI):
        chunk.append((mmsi, row))
        
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
            gc.collect()
    
    if chunk:
        yield chunk


def create_mmsi_partitioned_chunks(filepath, chunk_size, max_mmsi_per_chunk = 100) -> Generator[Dict[str, List[List[str]]], None, None]:
    """
    Generator that creates MMSI-partitioned chunks (max 100 different MMSI codes per chunk) for parallel processing.
    """
    current_chunk: Dict[str, List[List[str]]] = defaultdict(list)
    current_size = 0
    current_mmsi_count = 0
    
    for mmsi, row in stream_valid_rows(filepath, COL_MMSI):
        if mmsi not in current_chunk:
            current_mmsi_count += 1
        
        current_chunk[mmsi].append(row)
        current_size += 1

        if current_size >= chunk_size or current_mmsi_count >= max_mmsi_per_chunk:
            yield dict(current_chunk)
            current_chunk = defaultdict(list)
            current_size = 0
            current_mmsi_count = 0
            gc.collect()
    
    if current_chunk:
        yield dict(current_chunk)


# PARALLEL WORKERS FUNCTIONS
def worker_process(
    worker_id: int,
    task_queue: Queue,
    result_queue: Queue,
    stop_flag: Synchronized
) -> None:
    """
    Worker process that receives chunks and processes them for anomaly detection.
    Each worker owns a disjoint set of MMSIs (enforced by the coordinator's
    hash-based routing), so accumulation and anomaly detection are always complete.
    """
    processed_chunks = 0
    total_records = 0
    mmsi_data: Dict[str, List[List[str]]] = defaultdict(list)
    
    print(f"[Worker {worker_id}] Started")
    
    while not stop_flag.value:
        try:
            # Get chunk
            chunk = task_queue.get(timeout=0.1)
            
            if chunk is None:  # Poison pill
                break
            
            if isinstance(chunk, dict):
                for mmsi, rows in chunk.items():
                    mmsi_data[mmsi].extend(rows)
                    total_records += len(rows)
            else:
                for mmsi, row in chunk:
                    mmsi_data[mmsi].append(row)
                    total_records += 1
            
            processed_chunks += 1
            
            mem_mb = get_memory_usage_mb()
            if processed_chunks % 100 == 0:
                print(f"[Worker {worker_id}] Processed {processed_chunks} chunks, "
                      f"{total_records:,} records, Memory: {mem_mb:.1f} MB")
            
        except Exception as e:
            if "Empty" not in str(type(e).__name__):
                print(f"[Worker {worker_id}] Error: {e}")
            continue
    
    # sort each vessel's rows once before anomaly detection
    all_anomalies = []
    for mmsi, rows in mmsi_data.items():
        if rows:
            rows.sort(key=lambda r: parse_timestamp(r[COL_TIMESTAMP]))
            anomalies = detect_going_dark_anomalies(rows)
            all_anomalies.extend(anomalies)
    
    result_queue.put({
        'worker_id': worker_id,
        'processed_chunks': processed_chunks,
        'total_records': total_records,
        'unique_vessels': len(mmsi_data),
        'anomalies': all_anomalies,
        'final_memory_mb': get_memory_usage_mb()
    })
    
    print(f"[Worker {worker_id}] Finished - {total_records:,} records processed, {len(all_anomalies)} anomalies detected")


# MAIN PARALLEL PROCESSING COORDINATOR

class StreamingPartitioner:
    """
    Main coordinator class for parallel processing of AIS data.
    """
    
    def __init__(self, num_workers, chunk_size):
        self.num_workers = num_workers
        self.chunk_size = chunk_size
        self.worker_queues: List[Queue] = [
            Queue(maxsize=4) for _ in range(num_workers)
        ]
        self.result_queue: Queue = Queue()
        self.stop_flag = Value('b', False)
        self.workers: List[Process] = []
        
    def start_workers(self) -> None:
        """Start all worker processes."""
        for i in range(self.num_workers):
            worker = Process(
                target=worker_process,
                args=(i, self.worker_queues[i], self.result_queue, self.stop_flag)
            )
            worker.start()
            self.workers.append(worker)
        print(f"Started {self.num_workers} worker processes")
    
    def stop_workers(self) -> None:
        """Stop all worker processes gracefully."""
        self.stop_flag.value = True
        for q in self.worker_queues:
            q.put(None)
    
    def wait_for_workers(self) -> None:
        """Wait for all workers to finish."""
        for worker in self.workers:
            worker.join(timeout=30)
            if worker.is_alive():
                worker.terminate()
        self.workers = []
    
    def _route_chunk(self, chunk: Dict[str, List[List[str]]]) -> None:
        # Group rows by target worker
        worker_chunks: Dict[int, Dict[str, List[List[str]]]] = defaultdict(lambda: defaultdict(list))

        if isinstance(chunk, dict):
            for mmsi, rows in chunk.items():
                target = hash(mmsi) % self.num_workers
                worker_chunks[target][mmsi].extend(rows)
        else:
            for mmsi, row in chunk:
                target = hash(mmsi) % self.num_workers
                worker_chunks[target][mmsi].append(row)

        for worker_id, sub_chunk in worker_chunks.items():
            self.worker_queues[worker_id].put(dict(sub_chunk))

    def process_file(self, filepath: str, use_mmsi_partitioning: bool = True) -> Dict[str, Any]:
        """
        Process a CSV file using parallel workers.
        """
        start_time = time.time()
        file_size_gb = os.path.getsize(filepath) / (1024**3)
        
        print(f"\n{'='*70}")
        print(f"Processing: {os.path.basename(filepath)}")
        print(f"File size: {file_size_gb:.2f} GB")
        print(f"Workers: {self.num_workers}")
        print(f"Chunk size: {self.chunk_size:,} rows")
        print(f"{'='*70}\n")
        
        self.start_workers()
        
        chunks_sent = 0
        
        try:
            if use_mmsi_partitioning:
                chunk_generator = create_mmsi_partitioned_chunks(
                    filepath,
                    chunk_size=self.chunk_size
                )
            else:
                chunk_generator = create_chunks(filepath, chunk_size=self.chunk_size)
            
            for chunk in chunk_generator:
                self._route_chunk(chunk)
                chunks_sent += 1
                
                if chunks_sent % 100 == 0:
                    mem_mb = get_memory_usage_mb()
                    print(f"[Main] Dispatched {chunks_sent} chunks, Memory: {mem_mb:.1f} MB")
                    
                    if mem_mb > 800:
                        print("[Main] WARNING: Approaching memory limit, forcing GC")
                        gc.collect()
                        
        except KeyboardInterrupt:
            print("\n[Main] Interrupted by user")
        
        print(f"\n[Main] Finished streaming, sent {chunks_sent} chunks")

        self.stop_workers()
        aggregated_results = self._aggregate_results()
        self.wait_for_workers()

        end_time = time.time()
        elapsed = end_time - start_time
        
        aggregated_results['file'] = filepath
        aggregated_results['file_size_gb'] = file_size_gb
        aggregated_results['elapsed_seconds'] = elapsed
        aggregated_results['chunks_processed'] = chunks_sent
        aggregated_results['throughput_mb_per_sec'] = (file_size_gb * 1024) / elapsed if elapsed > 0 else 0
        
        self._print_summary(aggregated_results)
        
        return aggregated_results
    
    def _aggregate_results(self) -> Dict[str, Any]:
        """Collect and aggregate results from all workers."""
        worker_results = []
        total_records = 0
        all_anomalies = []
        all_mmsi_seen: set = set()
        
        results_received = 0
        while results_received < self.num_workers:
            try:
                result = self.result_queue.get(timeout=5.0)
                worker_results.append(result)
                total_records += result['total_records']
                all_anomalies.extend(result['anomalies'])

                # Accumulate unique MMSIs from each anomaly record
                # (workers already de-duplicate within themselves via mmsi_data keys)
                for anomaly in result['anomalies']:
                    all_mmsi_seen.add(anomaly['mmsi'])

                results_received += 1
                print(f"[Main] Received results from worker {result['worker_id']}: {len(result['anomalies'])} anomalies")
                    
            except Exception as e:
                print(f"[Main] Timeout waiting for worker results: {e}")
                break

        # Use the per-worker unique vessel counts, which are now guaranteed
        unique_vessels = sum(r['unique_vessels'] for r in worker_results)

        dfsi = len(all_anomalies)
        
        return {
            'worker_results': worker_results,
            'total_records': total_records,
            'unique_vessels': unique_vessels,
            'anomalies': all_anomalies,
            'total_anomalies': len(all_anomalies),
            'dfsi': dfsi,
            'max_memory_mb': max(r['final_memory_mb'] for r in worker_results) if worker_results else 0
        }
    
    def _print_summary(self, results: Dict[str, Any]) -> None:
        """Print a summary of processing results."""
        print(f"\n{'='*70}")
        print("SHADOW FLEET DETECTION SUMMARY")
        print(f"{'='*70}")
        print(f"File: {os.path.basename(results['file'])}")
        print(f"File size: {results['file_size_gb']:.2f} GB")
        print(f"Total records processed: {results['total_records']:,}")
        print(f"Unique vessels (MMSIs): {results['unique_vessels']:,}")
        print(f"Total anomalies detected: {results['total_anomalies']:,}")
        print(f"Shadow Fleet Suspicion Index (DFSI): {results['dfsi']}")
        print(f"Chunks processed: {results['chunks_processed']:,}")
        print(f"Elapsed time: {results['elapsed_seconds']:.2f} seconds")
        print(f"Throughput: {results['throughput_mb_per_sec']:.2f} MB/sec")
        print(f"Peak worker memory: {results['max_memory_mb']:.1f} MB")
        print(f"{'='*70}")
        
        if results['anomalies']:
            print("\nSample Anomalies (Going Dark):")
            print("-" * 70)
            for i, anomaly in enumerate(results['anomalies'][:5], 1):
                print(f"  {i}. MMSI {anomaly['mmsi']}: {anomaly['gap_hours']:.1f}h gap, "
                      f"{anomaly['distance_km']:.2f}km moved")
                print(f"     From {anomaly['gap_start']} to {anomaly['gap_end']}")
            if len(results['anomalies']) > 5:
                print(f"  ... and {len(results['anomalies']) - 5} more anomalies")


