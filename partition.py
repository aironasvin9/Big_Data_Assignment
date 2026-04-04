# partition.py
"""
Data partitioning for parallel processing.
"""

import gc
from typing import Generator, List, Tuple, Dict
from collections import defaultdict
from config import CHUNK_SIZE, MAX_MMSI_PER_CHUNK
from parsing import stream_valid_rows


def create_mmsi_partitioned_chunks(
    filepath: str,
    chunk_size: int = CHUNK_SIZE,
    max_mmsi_per_chunk: int = MAX_MMSI_PER_CHUNK,
) -> Generator[Dict[str, List[Tuple]], None, None]:
    """
    Generator that creates MMSI-partitioned chunks for parallel processing.
    Each chunk: {mmsi: [(ts_str, epoch, lat, lon, sog, draught), ...], ...}
    """
    current_chunk: Dict[str, List] = defaultdict(list)
    current_size = 0
    current_mmsi_count = 0

    for mmsi, ts_str, epoch, lat, lon, sog, draught in stream_valid_rows(filepath):
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


def route_chunk_to_workers(chunk: Dict, num_workers: int) -> Dict[int, Dict]:
    """Route a chunk's MMSIs to workers by hash(mmsi) % num_workers."""
    worker_chunks: Dict[int, Dict[str, List]] = defaultdict(lambda: defaultdict(list))
    
    for mmsi, records in chunk.items():
        worker_id = hash(mmsi) % num_workers
        worker_chunks[worker_id][mmsi].extend(records)
    
    return {worker_id: dict(sub_chunk) for worker_id, sub_chunk in worker_chunks.items()}
