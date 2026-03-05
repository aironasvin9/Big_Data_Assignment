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

INVALID_MMSI_PATTERNS = {
    '000000000',
    '111111111',
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
    Check wether MMSI code is valid according to standard rules.
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


# CHUNCK PARTITIONING
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
    Worker process that receives chunks and processes them:
        - Gets chunks from the task queue
        - Processes the data (counts records per MMSI, computes basic stats)
        - Puts results in the result queue
    """
    processed_chunks = 0
    total_records = 0
    mmsi_counts: Dict[str, int] = defaultdict(int)
    
    print(f"[Worker {worker_id}] Started")
    
    while not stop_flag.value:
        try:
            # Get chunk
            chunk = task_queue.get(timeout=0.1)
            
            if chunk is None:  # Poison pill
                break
            
            # Process the chunk
            if isinstance(chunk, dict):
                # MMSI-partitioned chunk
                for mmsi, rows in chunk.items():
                    mmsi_counts[mmsi] += len(rows)
                    total_records += len(rows)
            else:
                # Simple list of (mmsi, row) tuples
                for mmsi, row in chunk:
                    mmsi_counts[mmsi] += 1
                    total_records += 1
            
            processed_chunks += 1
            
            # Memory check
            mem_mb = get_memory_usage_mb()
            if processed_chunks % 100 == 0:
                print(f"[Worker {worker_id}] Processed {processed_chunks} chunks, "
                      f"{total_records:,} records, Memory: {mem_mb:.1f} MB")
            
        except Exception as e:
            if "Empty" not in str(type(e).__name__):
                print(f"[Worker {worker_id}] Error: {e}")
            continue
    
    result_queue.put({
        'worker_id': worker_id,
        'processed_chunks': processed_chunks,
        'total_records': total_records,
        'mmsi_counts': dict(mmsi_counts),
        'final_memory_mb': get_memory_usage_mb()
    })
    
    print(f"[Worker {worker_id}] Finished - {total_records:,} records processed")


# MAIN PARALLEL PROCESSING COORDINATOR
# =============================================================================

class StreamingPartitioner:
    """
    Main coordinator class for parallel processing of our data.
    """
    
    def __init__(self, num_workers, chunk_size):
        self.num_workers = num_workers
        self.chunk_size = chunk_size
        self.task_queue: Queue = Queue(maxsize=num_workers * 2)  # Limit queue size
        self.result_queue: Queue = Queue()
        self.stop_flag = Value('b', False)
        self.workers: List[Process] = []
        
    def start_workers(self) -> None:
        """Start all worker processes."""
        for i in range(self.num_workers):
            worker = Process(
                target=worker_process,
                args=(i, self.task_queue, self.result_queue, self.stop_flag)
            )
            worker.start()
            self.workers.append(worker)
        print(f"Started {self.num_workers} worker processes")
    
    def stop_workers(self) -> None:
        """Stop all worker processes gracefully."""
        # Signal workers to stop checking for new work
        self.stop_flag.value = True
        
        # Send poison pills
        for _ in self.workers:
            self.task_queue.put(None)
    
    def wait_for_workers(self) -> None:
        """Wait for all workers to finish."""
        for worker in self.workers:
            worker.join(timeout=30)
            if worker.is_alive():
                worker.terminate()
        self.workers = []
    
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
        
        # Start workers
        self.start_workers()
        
        # Stream chunks to workers
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
                self.task_queue.put(chunk)
                chunks_sent += 1
                
                if chunks_sent % 100 == 0:
                    mem_mb = get_memory_usage_mb()
                    print(f"[Main] Dispatched {chunks_sent} chunks, Memory: {mem_mb:.1f} MB")
                    
                    # Memory safety check
                    if mem_mb > 800:  # Approaching 1GB limit
                        print("[Main] WARNING: Approaching memory limit, forcing GC")
                        gc.collect()
                        
        except KeyboardInterrupt:
            print("\n[Main] Interrupted by user")
        
        print(f"\n[Main] Finished streaming, sent {chunks_sent} chunks")

        # First send poison pills to make workers finish and send results
        self.stop_workers()

        # Now collect results (workers are finishing and sending results)
        aggregated_results = self._aggregate_results()

        # Wait for workers to fully terminate
        self.wait_for_workers()

        end_time = time.time()
        
        elapsed = end_time - start_time
        
        # Add summary statistics
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
        combined_mmsi_counts: Dict[str, int] = defaultdict(int)
        
        # Wait for exactly num_workers results (one per worker)
        results_received = 0
        while results_received < self.num_workers:
            try:
                result = self.result_queue.get(timeout=5.0)
                worker_results.append(result)
                total_records += result['total_records']
                
                for mmsi, count in result['mmsi_counts'].items():
                    combined_mmsi_counts[mmsi] += count
                
                results_received += 1
                print(f"[Main] Received results from worker {result['worker_id']}")
                    
            except Exception as e:
                print(f"[Main] Timeout waiting for worker results: {e}")
                break
        
        return {
            'worker_results': worker_results,
            'total_records': total_records,
            'unique_vessels': len(combined_mmsi_counts),
            'mmsi_counts': dict(combined_mmsi_counts),
            'max_memory_mb': max(r['final_memory_mb'] for r in worker_results) if worker_results else 0
        }
    
    def _print_summary(self, results: Dict[str, Any]) -> None:
        """Print a summary of processing results."""
        print(f"\n{'='*70}")
        print("PROCESSING SUMMARY")
        print(f"{'='*70}")
        print(f"File: {os.path.basename(results['file'])}")
        print(f"File size: {results['file_size_gb']:.2f} GB")
        print(f"Total records processed: {results['total_records']:,}")
        print(f"Unique vessels (MMSIs): {results['unique_vessels']:,}")
        print(f"Chunks processed: {results['chunks_processed']:,}")
        print(f"Elapsed time: {results['elapsed_seconds']:.2f} seconds")
        print(f"Throughput: {results['throughput_mb_per_sec']:.2f} MB/sec")
        print(f"Peak worker memory: {results['max_memory_mb']:.1f} MB")
        print(f"{'='*70}")
        
        # Show top 10 most active vessels
        if results['mmsi_counts']:
            sorted_vessels = sorted(
                results['mmsi_counts'].items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10]
            print("\nTop 10 Most Active Vessels:")
            print("-" * 40)
            for i, (mmsi, count) in enumerate(sorted_vessels, 1):
                print(f"  {i:2}. MMSI {mmsi}: {count:,} records")
