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

# ============================================================================
# ANOMALY DETECTION HELPERS
# ============================================================================

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
    """
    Anomaly A: Detect AIS gaps >4 hours where vessel moved >5km (not anchored).
    
    Args:
        mmsi: Vessel MMSI
        records: List of (ts_str, epoch, lat, lon, sog, draught) tuples
        gap_threshold_hours: Minimum gap duration (default 4 hours)
        movement_threshold_km: Minimum movement to flag (default 5 km)
    
    Returns:
        List of going-dark anomalies
    """
    if len(records) < 2:
        return []

    anomalies = []
    gap_threshold_sec = gap_threshold_hours * 3600
    
    for i in range(1, len(records)):
        prev_ts_str, prev_epoch, prev_lat, prev_lon, _, _ = records[i - 1]
        curr_ts_str, curr_epoch, curr_lat, curr_lon, _, _ = records[i]

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


def detect_teleportation_anomalies(
    mmsi: str,
    records: List[Tuple],
    speed_threshold_knots: float = 60.0,
) -> List[Dict[str, Any]]:
    """
    Anomaly D: Detect impossible vessel movements (identity cloning / teleportation).
    
    Args:
        mmsi: Vessel MMSI
        records: List of (ts_str, epoch, lat, lon, sog, draught) tuples
        speed_threshold_knots: Maximum realistic speed (default 60 knots)
    
    Returns:
        List of teleportation anomalies
    """
    if len(records) < 2:
        return []

    anomalies = []

    for i in range(1, len(records)):
        prev_ts_str, prev_epoch, prev_lat, prev_lon, _, _ = records[i - 1]
        curr_ts_str, curr_epoch, curr_lat, curr_lon, _, _ = records[i]

        time_sec = curr_epoch - prev_epoch
        if time_sec <= 0:
            continue
        
        # Distance in km
        dist_km = haversine_distance(prev_lat, prev_lon, curr_lat, curr_lon)

        # Convert to speed (knots)
        speed_kmh = dist_km / (time_sec / 3600.0)
        speed_knots = speed_kmh / 1.852  # km/h -> knots

        if speed_knots > speed_threshold_knots:
            anomalies.append({
                'mmsi': mmsi,
                'gap_start': prev_ts_str,
                'gap_end': curr_ts_str,
                'distance_km': round(dist_km, 2),
                'distance_nm': round(dist_km / 1.852, 2),
                'speed_knots': round(speed_knots, 2),
                'pos_prev': (prev_lat, prev_lon),
                'pos_curr': (curr_lat, curr_lon),
                'anomaly_type': 'teleportation',
            })

    return anomalies


def detect_draft_change_anomalies(
    mmsi: str,
    records: List[Tuple],
    gap_threshold_hours: float = 2.0,
    draft_change_percent_threshold: float = 5.0,
) -> List[Dict[str, Any]]:
    """
    Anomaly C: Detect vessels whose draught changes >5% during AIS blackouts >2 hours.
    This implies cargo was loaded/unloaded illegally.
    
    Args:
        mmsi: Vessel MMSI
        records: List of (ts_str, epoch, lat, lon, sog, draught) tuples
        gap_threshold_hours: Minimum blackout duration (default 2 hours)
        draft_change_percent_threshold: Minimum % change to flag (default 5%)
    
    Returns:
        List of draft change anomalies
    """
    anomalies = []
    gap_threshold_sec = gap_threshold_hours * 3600
    
    for i in range(1, len(records)):
        prev_ts_str, prev_epoch, prev_lat, prev_lon, prev_sog, prev_draft = records[i - 1]
        curr_ts_str, curr_epoch, curr_lat, curr_lon, curr_sog, curr_draft = records[i]
        
        gap_sec = curr_epoch - prev_epoch
        
        # Must have a significant gap
        if gap_sec <= gap_threshold_sec:
            continue
        
        # Must have valid draught values
        if prev_draft <= 0 or curr_draft <= 0:
            continue
        
        # Calculate % change
        draft_change_percent = ((curr_draft - prev_draft) / prev_draft) * 100
        
        # Flag if change exceeds threshold
        if abs(draft_change_percent) >= draft_change_percent_threshold:
            anomalies.append({
                'mmsi': mmsi,
                'gap_start': prev_ts_str,
                'gap_end': curr_ts_str,
                'gap_hours': round(gap_sec / 3600.0, 2),
                'draught_before': round(prev_draft, 2),
                'draught_after': round(curr_draft, 2),
                'draught_change_percent': round(draft_change_percent, 2),
                'pos_before': (prev_lat, prev_lon),
                'pos_after': (curr_lat, curr_lon),
                'anomaly_type': 'draft_change',
            })
    
    return anomalies


def detect_loitering_anomalies(
    mmsi_records: Dict[str, List[Tuple]],
    proximity_threshold_km: float = 0.5,
    sog_threshold_knots: float = 1.0,
    loitering_duration_hours: float = 2.0,
) -> List[Dict[str, Any]]:
    """
    Anomaly B: Detect two distinct vessels within 500m of each other with 
    SOG <1 knot for >2 hours (possible ship-to-ship transfer).
    
    Args:
        mmsi_records: Dict mapping mmsi -> [(ts_str, epoch, lat, lon, sog, draught), ...]
        proximity_threshold_km: Distance threshold (default 0.5 km = 500m)
        sog_threshold_knots: Speed threshold (default 1 knot)
        loitering_duration_hours: Minimum loitering duration (default 2 hours)
    
    Returns:
        List of loitering anomalies
    """
    anomalies = []
    loitering_sec = loitering_duration_hours * 3600  # 7200 seconds for 2 hours
    mmsi_list = sorted(mmsi_records.keys())
    
    # Check all pairs of distinct vessels
    for idx1 in range(len(mmsi_list)):
        for idx2 in range(idx1 + 1, len(mmsi_list)):
            mmsi1 = mmsi_list[idx1]
            mmsi2 = mmsi_list[idx2]
            
            records1 = mmsi_records[mmsi1]
            records2 = mmsi_records[mmsi2]
            
            # Need at least 2 records per vessel (to detect movement/stationarity)
            if len(records1) < 2 or len(records2) < 2:
                continue
            
            # Records should already be sorted by epoch (worker does this)
            # Find overlapping time window
            min_epoch = max(records1[0][1], records2[0][1])
            max_epoch = min(records1[-1][1], records2[-1][1])
            
            # Must have at least 2 hours of overlap
            overlap_duration = max_epoch - min_epoch
            if overlap_duration < loitering_sec:
                continue
            
            # Now find periods where both vessels are close AND slow
            # Build a timeline of proximity events
            proximity_windows = []
            
            for i1, rec1 in enumerate(records1):
                ts1, epoch1, lat1, lon1, sog1, _ = rec1
                
                # Vessel 1 must be moving slowly
                if sog1 > sog_threshold_knots:
                    continue
                
                # Find records from vessel 2 that are close in time
                for i2, rec2 in enumerate(records2):
                    ts2, epoch2, lat2, lon2, sog2, _ = rec2
                    
                    # Time window: within ±15 minutes for matching pings
                    time_diff = abs(epoch1 - epoch2)
                    if time_diff > 900:  # 900 seconds = 15 minutes
                        continue
                    
                    # Vessel 2 must also be moving slowly
                    if sog2 > sog_threshold_knots:
                        continue
                    
                    # Calculate distance
                    dist_km = haversine_distance(lat1, lon1, lat2, lon2)
                    
                    # Must be close (within 500m)
                    if dist_km < proximity_threshold_km:
                        proximity_windows.append({
                            'epoch': epoch1,  # Use vessel 1's timestamp
                            'ts1': ts1,
                            'ts2': ts2,
                            'mmsi1': mmsi1,
                            'mmsi2': mmsi2,
                            'dist_km': dist_km,
                            'pos1': (lat1, lon1),
                            'pos2': (lat2, lon2),
                            'sog1': sog1,
                            'sog2': sog2,
                        })
            
            # Check if proximity windows span at least 2 hours
            if len(proximity_windows) < 3:
                continue
            
            proximity_windows.sort(key=lambda x: x['epoch'])
            
            # Find continuous loitering period
            # Check if from first to last proximity event >= 2 hours
            duration = proximity_windows[-1]['epoch'] - proximity_windows[0]['epoch']
            
            if duration >= loitering_sec:
                # Extract key moments
                first_event = proximity_windows[0]
                last_event = proximity_windows[-1]
                
                anomalies.append({
                    'mmsi_vessel1': mmsi1,
                    'mmsi_vessel2': mmsi2,
                    'anomaly_type': 'loitering',
                    'loitering_start': first_event['ts1'],
                    'loitering_end': last_event['ts1'],
                    'duration_hours': round(duration / 3600.0, 2),
                    'proximity_events': len(proximity_windows),
                    'min_distance_km': round(min(p['dist_km'] for p in proximity_windows), 3),
                    'location': first_event['pos1'],  # Location of first event
                    'vessel1_avg_sog': round(sum(p['sog1'] for p in proximity_windows) / len(proximity_windows), 2),
                    'vessel2_avg_sog': round(sum(p['sog2'] for p in proximity_windows) / len(proximity_windows), 2),
                })
    
    return anomalies


# ============================================================================
# DFSI CALCULATION
# ============================================================================

def calculate_dfsi(mmsi: str, anomalies_for_vessel: List[Dict[str, Any]]) -> float:
    """
    Calculate Dynamic Fictional Suspicion Index (DFSI) for a vessel.
    
    Formula:
    DFSI = (MAX_GAP_HOURS / 2) + (TOTAL_IMPOSSIBLE_DISTANCE_JUMPS / 10) + (C * 15)
    
    Where:
    - MAX_GAP_HOURS: Longest "going dark" gap in hours
    - TOTAL_IMPOSSIBLE_DISTANCE_JUMPS: Sum of all teleportation distances in nautical miles
    - C: Count of draft change anomalies
    
    Args:
        mmsi: Vessel MMSI
        anomalies_for_vessel: List of all anomalies for this vessel
    
    Returns:
        DFSI score (float)
    """
    # Extract anomaly-specific data
    going_dark_anomalies = [a for a in anomalies_for_vessel if a.get('anomaly_type') == 'going_dark']
    teleportation_anomalies = [a for a in anomalies_for_vessel if a.get('anomaly_type') == 'teleportation']
    draft_change_anomalies = [a for a in anomalies_for_vessel if a.get('anomaly_type') == 'draft_change']
    
    # Component 1: Max gap in hours
    max_gap_hours = 0.0
    if going_dark_anomalies:
        max_gap_hours = max(a['gap_hours'] for a in going_dark_anomalies)
    
    # Component 2: Total impossible distance in nautical miles
    total_impossible_distance_nm = 0.0
    if teleportation_anomalies:
        # Use pre-calculated distance_nm if available, otherwise convert
        total_impossible_distance_nm = sum(
            a.get('distance_nm', a['distance_km'] / 1.852) for a in teleportation_anomalies
        )
    
    # Component 3: Count of draft changes
    draft_change_count = len(draft_change_anomalies)
    
    # Calculate DFSI
    dfsi = (max_gap_hours / 2.0) + (total_impossible_distance_nm / 10.0) + (draft_change_count * 15.0)
    
    return round(dfsi, 2)


def aggregate_anomalies_by_vessel(all_anomalies: List[Dict[str, Any]]) -> Dict[str, Dict]:
    """
    Organize anomalies by MMSI and calculate DFSI for each vessel.
    
    Args:
        all_anomalies: Flat list of all detected anomalies from all workers
    
    Returns:
        Dict mapping mmsi -> {
            'anomalies': [list],
            'anomaly_counts': {type -> count},
            'dfsi': float
        }
    """
    vessels_dict: Dict[str, Dict] = defaultdict(lambda: {
        'anomalies': [],
        'anomaly_counts': defaultdict(int),
        'dfsi': 0.0
    })
    
    # Group anomalies by MMSI
    for anomaly in all_anomalies:
        mmsi = anomaly.get('mmsi')
        if not mmsi:
            continue
        
        vessels_dict[mmsi]['anomalies'].append(anomaly)
        anomaly_type = anomaly.get('anomaly_type', 'unknown')
        vessels_dict[mmsi]['anomaly_counts'][anomaly_type] += 1
    
    # Calculate DFSI for each vessel
    for mmsi, vessel_data in vessels_dict.items():
        vessel_data['dfsi'] = calculate_dfsi(mmsi, vessel_data['anomalies'])
        # Convert defaultdict to regular dict
        vessel_data['anomaly_counts'] = dict(vessel_data['anomaly_counts'])
    
    return dict(vessels_dict)


def rank_vessels_by_dfsi(vessels_dict: Dict[str, Dict], top_n: int = 50) -> List[Dict]:
    """
    Rank vessels by DFSI score and return top N.
    
    Args:
        vessels_dict: Output from aggregate_anomalies_by_vessel()
        top_n: Number of top vessels to return
    
    Returns:
        List of vessels sorted by DFSI (descending), with all anomaly details
    """
    ranked_vessels = []
    for mmsi, vessel_data in vessels_dict.items():
        ranked_vessels.append({
            'mmsi': mmsi,
            'dfsi': vessel_data['dfsi'],
            'anomaly_counts': vessel_data['anomaly_counts'],
            'anomalies': vessel_data['anomalies'],
            'total_anomalies': len(vessel_data['anomalies']),
        })
    
    # Sort by DFSI descending
    ranked_vessels.sort(key=lambda x: x['dfsi'], reverse=True)
    
    return ranked_vessels[:top_n]


# ============================================================================
# MEMORY MONITORING UTILITIES
# ============================================================================

def get_memory_usage_mb():
    """Get current memory usage in MB (macOS/Linux)."""
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_maxrss / (1024 * 1024)  # Convert to MB on macOS
    except Exception:
        return 0.0


def check_memory_limit(limit_mb=1000.0):
    """Check if memory usage is under the limit (default 1GB)."""
    current = get_memory_usage_mb()
    return current < limit_mb


# ============================================================================
# DATA VALIDATION & FILTERING
# ============================================================================

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


# ============================================================================
# STREAMING DATA GENERATOR
# ============================================================================

def stream_csv_rows(filepath, skip_header=True) -> Generator[List[str], None, None]:
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
    """
    Generator that streams valid rows with extended data.
    Yields: (mmsi, ts_str, epoch, lat, lon, sog, draught)
    """
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
            
            # Extract SOG (Speed Over Ground) and Draught
            sog = 0.0
            if len(row) > COL_SOG:
                try:
                    sog = float(row[COL_SOG])
                except (ValueError, TypeError):
                    sog = 0.0
            
            draught = 0.0
            if len(row) > COL_DRAUGHT:
                try:
                    draught = float(row[COL_DRAUGHT])
                except (ValueError, TypeError):
                    draught = 0.0
            
        except (ValueError, IndexError):
            continue
        
        yield (mmsi, ts_str, epoch, lat, lon, sog, draught)


# ============================================================================
# CHUNK PARTITIONING
# ============================================================================

def create_chunks(filepath, chunk_size) -> Generator[List[Tuple], None, None]:
    """
    Generator that creates flat chunks of lightweight records.
    Each item: (mmsi, ts_str, epoch_sec, lat, lon, sog, draught).
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
    Each chunk: {mmsi: [(ts_str, epoch_sec, lat, lon, sog, draught), ...], ...}
    At most max_mmsi_per_chunk distinct vessels appear in each yielded dict.
    """
    current_chunk: Dict[str, List] = defaultdict(list)
    current_size = 0
    current_mmsi_count = 0

    for mmsi, ts_str, epoch, lat, lon, sog, draught in stream_valid_rows(filepath, COL_MMSI):
        if mmsi not in current_chunk:
            current_mmsi_count += 1
        current_chunk[mmsi].append((ts_str, epoch, lat, lon, sog, draught))
        current_size += 1

        if current_size >= chunk_size or current_mmsi_count >= max_mmsi_per_chunk:
            yield dict(current_chunk)
            current_chunk = defaultdict(list)
            current_size = 0
            current_mmsi_count = 0
            gc.collect()

    if current_chunk:
        yield dict(current_chunk)


# ============================================================================
# PARALLEL WORKERS FUNCTIONS
# ============================================================================

def worker_process(
    worker_id: int,
    task_queue: Queue,
    result_queue: Queue,
    stop_flag: Synchronized,
) -> None:
    """
    Worker process that accumulates records and detects anomalies.
    """
    processed_chunks = 0
    total_records = 0
    mmsi_data: Dict[str, List[Tuple]] = defaultdict(list)

    print(f"[Worker {worker_id}] Started")

    # Phase 1: accumulate
    while not stop_flag.value:
        try:
            chunk = task_queue.get(timeout=0.2)
            if chunk is None:
                break

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

    # Phase 2: sort + detect anomalies
    all_anomalies: List[Dict[str, Any]] = []
    for mmsi, records in mmsi_data.items():
        if len(records) < 2:
            continue
        records.sort(key=lambda r: r[1])
        
        all_anomalies.extend(detect_going_dark_anomalies(mmsi, records))
        all_anomalies.extend(detect_teleportation_anomalies(mmsi, records))
        all_anomalies.extend(detect_draft_change_anomalies(mmsi, records))

    result_queue.put({
        'worker_id': worker_id,
        'processed_chunks': processed_chunks,
        'total_records': total_records,
        'mmsi_counts': {mmsi: len(recs) for mmsi, recs in mmsi_data.items()},
        'anomalies': all_anomalies,
        'mmsi_records': dict(mmsi_data),  # ⭐ ADD THIS LINE
        'final_memory_mb': get_memory_usage_mb(),
    })

    going_dark_count = sum(1 for a in all_anomalies if a.get('anomaly_type') == 'going_dark')
    teleportation_count = sum(1 for a in all_anomalies if a.get('anomaly_type') == 'teleportation')
    draft_count = sum(1 for a in all_anomalies if a.get('anomaly_type') == 'draft_change')
    
    print(
        f"[Worker {worker_id}] Finished — {total_records:,} records, "
        f"{len(all_anomalies)} total anomalies "
        f"(Going Dark: {going_dark_count}, Teleport: {teleportation_count}, Draft: {draft_count})"
    )


# ============================================================================
# MAIN PARALLEL PROCESSING COORDINATOR
# ============================================================================

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
            # flat list: (mmsi, ts_str, epoch, lat, lon, sog, draught)
            for mmsi, ts_str, epoch, lat, lon, sog, draught in chunk:
                worker_chunks[hash(mmsi) % self.num_workers][mmsi].append((ts_str, epoch, lat, lon, sog, draught))

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
        
        # Calculate DFSI rankings
        aggregated_results = self._calculate_dfsi_rankings(aggregated_results)

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

    def _calculate_dfsi_rankings(self, aggregated_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Post-process anomalies to calculate DFSI and rank vessels.
        
        Args:
            aggregated_results: Results from _aggregate_results()
        
        Returns:
            Enhanced results with DFSI rankings
        """
        all_anomalies = aggregated_results.get('anomalies', [])
        
        # Aggregate anomalies by vessel and calculate DFSI
        vessels_dict = aggregate_anomalies_by_vessel(all_anomalies)
        
        # Get top 50 most suspicious vessels
        top_vessels = rank_vessels_by_dfsi(vessels_dict, top_n=50)
        
        # Add to results
        aggregated_results['vessels_by_dfsi'] = top_vessels
        aggregated_results['total_flagged_vessels'] = len(vessels_dict)
        
        # Calculate summary statistics
        all_dfsi_scores = [v['dfsi'] for v in top_vessels]
        aggregated_results['dfsi_stats'] = {
            'mean': round(sum(all_dfsi_scores) / len(all_dfsi_scores), 2) if all_dfsi_scores else 0,
            'max': max(all_dfsi_scores) if all_dfsi_scores else 0,
            'min': min(all_dfsi_scores) if all_dfsi_scores else 0,
        }
        
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

        # Show anomalies by type
        anomalies = results.get('anomalies', [])
        going_dark_anomalies = [a for a in anomalies if a.get('anomaly_type') == 'going_dark']
        teleportation_anomalies = [a for a in anomalies if a.get('anomaly_type') == 'teleportation']
        draft_change_anomalies = [a for a in anomalies if a.get('anomaly_type') == 'draft_change']
        
        print(f"\n{'='*70}")
        print("ANOMALIES SUMMARY")
        print(f"{'='*70}")
        print(f"Going-Dark Anomalies (A): {len(going_dark_anomalies)}")
        print(f"Teleportation Anomalies (D): {len(teleportation_anomalies)}")
        print(f"Draft Change Anomalies (C): {len(draft_change_anomalies)}")
        print(f"Total Anomalies: {len(anomalies)}")

        if going_dark_anomalies:
            # Sort by longest gap first
            top_anomalies = sorted(going_dark_anomalies, key=lambda a: a['gap_hours'], reverse=True)[:5]
            print(f"\nTop 5 Going-Dark Events:")
            print("-" * 60)
            print(f"  {'MMSI':<12} {'Gap (h)':>8}  {'Distance (km)':>14}  Gap window")
            print("-" * 60)
            for a in top_anomalies:
                print(
                    f"  {a['mmsi']:<12} {a['gap_hours']:>8.1f}  "
                    f"{a['distance_km']:>14.1f}  "
                    f"{a['gap_start']} → {a['gap_end']}"
                )

        # Show DFSI Rankings
        if 'vessels_by_dfsi' in results:
            top_vessels = results['vessels_by_dfsi'][:10]
            print(f"\n{'='*70}")
            print("TOP 10 SHADOW FLEET SUSPECTS (by DFSI)")
            print(f"{'='*70}")
            print(f"{'Rank':<6} {'MMSI':<12} {'DFSI':>8} {'Dark':>6} {'Teleport':>10} {'Draft':>6}")
            print("-" * 70)
            for i, vessel in enumerate(top_vessels, 1):
                counts = vessel['anomaly_counts']
                print(
                    f"{i:<6} {vessel['mmsi']:<12} {vessel['dfsi']:>8.2f} "
                    f"{counts.get('going_dark', 0):>6} "
                    f"{counts.get('teleportation', 0):>10} "
                    f"{counts.get('draft_change', 0):>6}"
                )
            
            print(f"\nDFSI Statistics (Top 50):")
            print(f"  Mean DFSI: {results['dfsi_stats']['mean']:.2f}")
            print(f"  Max DFSI: {results['dfsi_stats']['max']:.2f}")
            print(f"  Min DFSI: {results['dfsi_stats']['min']:.2f}")
            print(f"  Total flagged vessels: {results.get('total_flagged_vessels', 0)}")
