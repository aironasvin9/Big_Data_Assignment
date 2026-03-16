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
    '222222222',
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

# ANOMALY DETECTION HELPERS

def parse_timestamp(ts_str: str) -> datetime.datetime:
    """Parse AIS timestamp in format DD/MM/YYYY HH:MM:SS."""
    return datetime.datetime.strptime(ts_str.strip(), "%d/%m/%Y %H:%M:%S")


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in kilometres between two points."""
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


# Reference point for converting timestamps to compact integers (fast sort key)
_EPOCH = datetime.datetime(2000, 1, 1)


def ts_to_epoch(ts_str: str) -> int:
    """Parse AIS timestamp (DD/MM/YYYY HH:MM:SS) to integer seconds since 2000-01-01."""
    dt = datetime.datetime.strptime(ts_str.strip(), "%d/%m/%Y %H:%M:%S")
    return int((dt - _EPOCH).total_seconds())


def detect_going_dark_anomalies(
    mmsi: str,
    records: List[Tuple],
    gap_threshold_hours: float = 4.0,
    movement_threshold_km: float = 5.0,
) -> List[Dict[str, Any]]:
        
    if len(records) < 2:
        return []

    anomalies = []
    gap_threshold_sec = gap_threshold_hours * 3600
    for i in range(1, len(records)):
        prev_ts_str, prev_epoch, prev_lat, prev_lon = records[i - 1]
        curr_ts_str, curr_epoch, curr_lat, curr_lon = records[i]

        gap_sec = curr_epoch - prev_epoch
        if gap_sec <= gap_threshold_sec:
            continue

        dist_km = haversine_distance(prev_lat, prev_lon, curr_lat, curr_lon)
        if dist_km <= movement_threshold_km:
            continue  # Vessel did not move — likely anchored, not suspicious

        anomalies.append({
            'mmsi': mmsi,
            'gap_start': prev_ts_str,
            'gap_end': curr_ts_str,
            'gap_hours': round(gap_sec / 3600.0, 2),
            'distance_km': round(dist_km, 2),
            'pos_before': (prev_lat, prev_lon),
            'pos_after': (curr_lat, curr_lon),
            'anomaly_type': 'going_dark',
        })

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


def stream_valid_rows(filepath, COL_MMSI) -> Generator[Tuple, None, None]:
    row_generator = stream_csv_rows(filepath, skip_header=True)

    try:
        _header = next(row_generator)
    except StopIteration:
        return

    for row in row_generator:
        if not validate_row(row, COL_MMSI, COL_LATITUDE, COL_LONGITUDE):
            continue
        mmsi = row[COL_MMSI].strip()
        try:
            ts_str = row[COL_TIMESTAMP]
            epoch = ts_to_epoch(ts_str)
            lat = float(row[COL_LATITUDE])
            lon = float(row[COL_LONGITUDE])
        except (ValueError, IndexError):
            continue
        yield (mmsi, ts_str, epoch, lat, lon)


# CHUNK PARTITIONING
def create_chunks(filepath, chunk_size) -> Generator[List[Tuple], None, None]:
    """
    Generator that creates flat chunks of lightweight records.
    Each item: (mmsi, ts_str, epoch_sec, lat, lon).
    """
    chunk = []

    for record in stream_valid_rows(filepath, COL_MMSI):
        chunk.append(record)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
            gc.collect()

    if chunk:
        yield chunk


def create_mmsi_partitioned_chunks(filepath, chunk_size, max_mmsi_per_chunk=100) -> Generator[Dict[str, List[Tuple]], None, None]:
    """
    Generator that creates MMSI-partitioned chunks for parallel processing.
    Each chunk: {mmsi: [(ts_str, epoch_sec, lat, lon), ...], ...}
    At most max_mmsi_per_chunk distinct vessels appear in each yielded dict.
    """
    current_chunk: Dict[str, List] = defaultdict(list)
    current_size = 0
    current_mmsi_count = 0

    for mmsi, ts_str, epoch, lat, lon in stream_valid_rows(filepath, COL_MMSI):
        if mmsi not in current_chunk:
            current_mmsi_count += 1
        current_chunk[mmsi].append((ts_str, epoch, lat, lon))
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
    stop_flag: Synchronized,
) -> None:

    processed_chunks = 0
    total_records = 0
    # Dict[mmsi -> list of (ts_str, epoch_sec, lat, lon)]
    mmsi_data: Dict[str, List[Tuple]] = defaultdict(list)

    print(f"[Worker {worker_id}] Started")

    # ----- Phase 1: accumulate ------------------------------------------------
    while not stop_flag.value:
        try:
            chunk = task_queue.get(timeout=0.2)

            if chunk is None:  # Poison pill
                break

            # chunk is always a dict {mmsi: [(ts_str, epoch, lat, lon), ...]}
            for mmsi, records in chunk.items():
                mmsi_data[mmsi].extend(records)
                total_records += len(records)

            processed_chunks += 1

            if processed_chunks % 100 == 0:
                print(
                    f"[Worker {worker_id}] {processed_chunks} chunks, "
                    f"{total_records:,} records, {len(mmsi_data)} vessels"
                )

        except Exception as e:
            if "Empty" not in str(type(e).__name__):
                print(f"[Worker {worker_id}] Error: {e}")
            continue

    # ----- Phase 2: sort + detect anomalies -----------------------------------
    all_anomalies: List[Dict[str, Any]] = []
    for mmsi, records in mmsi_data.items():
        if len(records) < 2:
            continue
        # Sort by pre-parsed integer epoch — no datetime parsing needed here
        records.sort(key=lambda r: r[1])
        all_anomalies.extend(detect_going_dark_anomalies(mmsi, records))

    result_queue.put({
        'worker_id': worker_id,
        'processed_chunks': processed_chunks,
        'total_records': total_records,
        'mmsi_counts': {mmsi: len(recs) for mmsi, recs in mmsi_data.items()},
        'anomalies': all_anomalies,
        'final_memory_mb': get_memory_usage_mb(),
    })

    print(
        f"[Worker {worker_id}] Finished — {total_records:,} records, "
        f"{len(all_anomalies)} going-dark anomalies detected"
    )


# MAIN PARALLEL PROCESSING COORDINATOR
# =============================================================================

class StreamingPartitioner:
    """
    Main coordinator class for parallel processing of our data.

    Each worker owns a dedicated queue and receives only the MMSI groups that
    hash to its id, guaranteeing that every record for a given vessel is
    processed by a single worker — a prerequisite for chronological anomaly
    detection.
    """

    def __init__(self, num_workers, chunk_size):
        self.num_workers = num_workers
        self.chunk_size = chunk_size
        # One bounded queue per worker instead of a single shared queue
        self.worker_queues: List[Queue] = [Queue(maxsize=8) for _ in range(num_workers)]
        self.result_queue: Queue = Queue()
        self.stop_flag = Value('b', False)
        self.workers: List[Process] = []

    def start_workers(self) -> None:
        """Start all worker processes."""
        for i in range(self.num_workers):
            worker = Process(
                target=worker_process,
                args=(i, self.worker_queues[i], self.result_queue, self.stop_flag),
            )
            worker.start()
            self.workers.append(worker)
        print(f"Started {self.num_workers} worker processes")

    def stop_workers(self) -> None:
        """Stop all worker processes gracefully."""
        self.stop_flag.value = True
        for q in self.worker_queues:
            q.put(None)

    def _route_chunk(self, chunk) -> None:
        """Route lightweight records to workers by hash(mmsi) % num_workers."""
        worker_chunks: Dict[int, Dict[str, List]] = defaultdict(lambda: defaultdict(list))
        if isinstance(chunk, dict):
            for mmsi, records in chunk.items():
                worker_chunks[hash(mmsi) % self.num_workers][mmsi].extend(records)
        else:
            # flat list: (mmsi, ts_str, epoch, lat, lon)
            for mmsi, ts_str, epoch, lat, lon in chunk:
                worker_chunks[hash(mmsi) % self.num_workers][mmsi].append((ts_str, epoch, lat, lon))

        for worker_id, sub_chunk in worker_chunks.items():
            self.worker_queues[worker_id].put(dict(sub_chunk))
    
    def wait_for_workers(self) -> None:
        """Wait for all workers to finish."""
        for worker in self.workers:
            worker.join(timeout=180)
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
                self._route_chunk(chunk)
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
        all_anomalies: List[Dict[str, Any]] = []

        results_received = 0
        while results_received < self.num_workers:
            try:
                result = self.result_queue.get(timeout=60.0)
                worker_results.append(result)
                total_records += result['total_records']

                for mmsi, count in result['mmsi_counts'].items():
                    combined_mmsi_counts[mmsi] += count

                all_anomalies.extend(result.get('anomalies', []))

                results_received += 1
                print(
                    f"[Main] Worker {result['worker_id']} — "
                    f"{result['total_records']:,} records, "
                    f"{len(result.get('anomalies', []))} anomalies"
                )

            except Exception as e:
                print(f"[Main] Timeout waiting for worker results: {e}")
                break

        return {
            'worker_results': worker_results,
            'total_records': total_records,
            'unique_vessels': len(combined_mmsi_counts),
            'mmsi_counts': dict(combined_mmsi_counts),
            'anomalies': all_anomalies,
            'max_memory_mb': max(r['final_memory_mb'] for r in worker_results) if worker_results else 0,
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
                reverse=True,
            )[:10]
            print("\nTop 10 Most Active Vessels:")
            print("-" * 40)
            for i, (mmsi, count) in enumerate(sorted_vessels, 1):
                print(f"  {i:2}. MMSI {mmsi}: {count:,} records")

        # Show going-dark anomalies
        anomalies = results.get('anomalies', [])
        print(f"\nGoing-Dark Anomalies Detected: {len(anomalies)}")
        if anomalies:
            # Sort by longest gap first
            top_anomalies = sorted(anomalies, key=lambda a: a['gap_hours'], reverse=True)[:10]
            print("-" * 60)
            print(f"  {'MMSI':<12} {'Gap (h)':>8}  {'Distance (km)':>14}  Gap window")
            print("-" * 60)
            for a in top_anomalies:
                print(
                    f"  {a['mmsi']:<12} {a['gap_hours']:>8.1f}  "
                    f"{a['distance_km']:>14.1f}  "
                    f"{a['gap_start']} → {a['gap_end']}"
                )
