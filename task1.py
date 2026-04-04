"""
Command-line interface for Two-Pass Shadow Fleet Detection.
Pass 1: Parallel detection (A, C, D)
Pass 2: Loitering detection (B) - BATCH VERSION (adjusted)
"""

import csv
import os
import time
import gc
import json
import resource
import multiprocessing as mp
from typing import Dict, Any, List, Tuple
from collections import defaultdict
from multiprocessing import Process, Queue, Value
from multiprocessing.sharedctypes import Synchronized

from config import (
    NUM_WORKERS, CHUNK_SIZE, ANALYSIS_DIR, OUTPUT_DIRS,
    TOP_N_GOING_DARK, TOP_N_VESSELS
)
from partition import create_mmsi_partitioned_chunks, route_chunk_to_workers
from detect import (
    detect_going_dark_anomalies,
    detect_teleportation_anomalies,
    detect_draft_change_anomalies,
)
from loiter import detect_loitering_anomalies  # 👈 changed import
from scoring import (
    calculate_dfsi,
    aggregate_anomalies_by_vessel,
    rank_vessels_by_dfsi,
)
from parsing import stream_csv_rows, is_valid_mmsi, stream_valid_rows
from geo import is_valid_coordinate


# =====================================================================
# UTIL
# =====================================================================
def get_memory_usage_mb() -> float:
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_maxrss / (1024 * 1024)
    except Exception:
        return 0.0


# =====================================================================
# WORKER PROCESS (UNCHANGED)
# =====================================================================
def worker_process(
    worker_id: int,
    task_queue: Queue,
    result_queue: Queue,
    stop_flag: Synchronized,
) -> None:

    processed_chunks = 0
    total_records = 0
    mmsi_data: Dict[str, List[Tuple]] = defaultdict(list)

    print(f"[Worker {worker_id}] Started (Pass 1)")

    while not stop_flag.value:
        try:
            chunk = task_queue.get(timeout=0.2)
            if chunk is None:
                break

            for mmsi, records in chunk.items():
                mmsi_data[mmsi].extend(records)
                total_records += len(records)

            processed_chunks += 1

        except Exception:
            continue

    print(f"[Worker {worker_id}] Starting anomaly detection...")

    all_anomalies = []

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
        'mmsi_counts': {m: len(r) for m, r in mmsi_data.items()},
        'anomalies': all_anomalies,
        'final_memory_mb': get_memory_usage_mb(),
    })

    print(f"[Worker {worker_id}] Finished — {len(all_anomalies)} anomalies")


# =====================================================================
# PIPELINE
# =====================================================================
class AISPipeline:

    def __init__(self, num_workers: int = NUM_WORKERS, chunk_size: int = CHUNK_SIZE):
        self.num_workers = num_workers
        self.chunk_size = chunk_size

        self.worker_queues: List[Queue] = [Queue(maxsize=8) for _ in range(num_workers)]
        self.result_queue: Queue = Queue()
        self.stop_flag = Value('b', False)
        self.workers: List[Process] = []

        for dir_path in OUTPUT_DIRS:
            os.makedirs(dir_path, exist_ok=True)

    def start_workers(self) -> None:
        for i in range(self.num_workers):
            worker = Process(
                target=worker_process,
                args=(i, self.worker_queues[i], self.result_queue, self.stop_flag),
            )
            worker.start()
            self.workers.append(worker)

    def stop_workers(self) -> None:
        self.stop_flag.value = True
        for q in self.worker_queues:
            q.put(None)

    def _route_chunk(self, chunk: Dict) -> None:
        worker_chunks = route_chunk_to_workers(chunk, self.num_workers)
        for worker_id, sub_chunk in worker_chunks.items():
            self.worker_queues[worker_id].put(sub_chunk)

    def wait_for_workers(self) -> None:
        for worker in self.workers:
            worker.join()
        self.workers = []

    # =================================================================
    def process_file(self, filepath: str) -> Dict[str, Any]:

        start_time = time.time()

        print("\n" + "="*70)
        print("PASS 1: Parallel Detection (A, C, D)")
        print("="*70 + "\n")

        pass1_start = time.time()

        self.start_workers()

        chunks_sent = 0

        for chunk in create_mmsi_partitioned_chunks(filepath, self.chunk_size):
            self._route_chunk(chunk)
            chunks_sent += 1

            if chunks_sent % 100 == 0:
                print(f"[Main] Dispatched {chunks_sent} chunks")

        print(f"\n[Main] Finished streaming, sent {chunks_sent} chunks")

        self.stop_workers()

        pass1_results = self._aggregate_results()
        self.wait_for_workers()

        pass1_time = time.time() - pass1_start

        print(f"\n[Main] PASS 1 completed in {pass1_time:.2f} seconds")

        # =================================================================
        # PASS 2 — 🔥 ONLY PART WE CHANGED
        # =================================================================
        print("\n" + "="*70)
        print("PASS 2: Loitering Detection (Batch Version)")
        print("="*70 + "\n")

        pass2_start = time.time()

        print("[Main] Building MMSI records...")

        mmsi_records = defaultdict(list)

        for mmsi, ts_str, epoch, lat, lon, sog, draught in stream_valid_rows(filepath):
            mmsi_records[mmsi].append((ts_str, epoch, lat, lon, sog, draught))

        for recs in mmsi_records.values():
            recs.sort(key=lambda r: r[1])

        print(f"[Main] Built records for {len(mmsi_records)} vessels")

        loitering_anomalies = detect_loitering_anomalies(mmsi_records)

        pass2_time = time.time() - pass2_start

        print(f"\n[Main] PASS 2 completed in {pass2_time:.2f} seconds")

        # =================================================================
        # PASS 3
        # =================================================================
        print("\n" + "="*70)
        print("PASS 3: Scoring & Ranking")
        print("="*70 + "\n")

        all_anomalies = pass1_results['anomalies'] + loitering_anomalies

        vessels_dict = aggregate_anomalies_by_vessel(all_anomalies)
        top_vessels = rank_vessels_by_dfsi(vessels_dict, TOP_N_VESSELS)

        end_time = time.time()
        elapsed = end_time - start_time

        print("\n=== DONE ===")
        print(f"Total time: {elapsed:.2f}s")
        print(f"Total anomalies: {len(all_anomalies)}")

        return {
            'pass1_seconds': pass1_time,
            'pass2_seconds': pass2_time,
            'total': elapsed,
        }

    # =================================================================
    def _aggregate_results(self) -> Dict[str, Any]:

        total_records = 0
        combined_mmsi_counts = defaultdict(int)
        all_anomalies = []

        results_received = 0

        while results_received < self.num_workers:
            result = self.result_queue.get()

            total_records += result['total_records']

            for mmsi, count in result['mmsi_counts'].items():
                combined_mmsi_counts[mmsi] += count

            all_anomalies.extend(result['anomalies'])

            results_received += 1

        return {
            'total_records': total_records,
            'unique_vessels': len(combined_mmsi_counts),
            'mmsi_counts': dict(combined_mmsi_counts),
            'anomalies': all_anomalies,
        }


# =====================================================================
# MAIN
# =====================================================================
def main():

    data_dir = "./data"

    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]

    if not csv_files:
        print("No CSV files found.")
        return

    filepath = os.path.join(data_dir, csv_files[0])

    pipeline = AISPipeline()
    pipeline.process_file(filepath)


if __name__ == "__main__":
    main()
