"""
Command-line interface for Two-Pass Shadow Fleet Detection.

PASS 1: Parallel detection (A, C, D)
PASS 2: Loitering detection (B) - BATCH VERSION
PASS 3: Scoring & Output
"""

import os
import time
import gc
import csv
import resource
import multiprocessing as mp

from typing import Dict, Any, List, Tuple
from collections import defaultdict
from multiprocessing import Process, Queue, Value
from multiprocessing.sharedctypes import Synchronized

from config import (
    NUM_WORKERS, CHUNK_SIZE, ANALYSIS_DIR, OUTPUT_DIRS,
    TOP_N_VESSELS
)

from partition import create_mmsi_partitioned_chunks, route_chunk_to_workers

from detect import (
    detect_going_dark_anomalies,
    detect_teleportation_anomalies,
    detect_draft_change_anomalies,
)

from loiter import detect_loitering_anomalies

from scoring import (
    aggregate_anomalies_by_vessel,
    rank_vessels_by_dfsi,
)

from parsing import stream_valid_rows


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
# WORKER PROCESS (PASS 1)
# =====================================================================
def worker_process(
    worker_id: int,
    task_queue: Queue,
    result_queue: Queue,
    stop_flag: Synchronized,
) -> None:

    mmsi_data: Dict[str, List[Tuple]] = defaultdict(list)
    total_records = 0

    print(f"[Worker {worker_id}] Started")

    while not stop_flag.value:
        try:
            chunk = task_queue.get(timeout=0.2)
            if chunk is None:
                break

            for mmsi, records in chunk.items():
                mmsi_data[mmsi].extend(records)
                total_records += len(records)

        except Exception:
            continue

    print(f"[Worker {worker_id}] Detecting anomalies...")

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
        'total_records': total_records,
        'anomalies': all_anomalies,
        'mmsi_counts': {m: len(r) for m, r in mmsi_data.items()},
        'memory': get_memory_usage_mb()
    })

    print(f"[Worker {worker_id}] Done — {len(all_anomalies)} anomalies")


# =====================================================================
# PIPELINE
# =====================================================================
class AISPipeline:

    def __init__(self, num_workers=NUM_WORKERS, chunk_size=CHUNK_SIZE):
        self.num_workers = num_workers
        self.chunk_size = chunk_size

        self.worker_queues = [Queue(maxsize=8) for _ in range(num_workers)]
        self.result_queue = Queue()
        self.stop_flag = Value('b', False)
        self.workers: List[Process] = []

        for d in OUTPUT_DIRS:
            os.makedirs(d, exist_ok=True)

    # --------------------------------------------------
    def start_workers(self):
        for i in range(self.num_workers):
            p = Process(
                target=worker_process,
                args=(i, self.worker_queues[i], self.result_queue, self.stop_flag),
            )
            p.start()
            self.workers.append(p)

    # --------------------------------------------------
    def stop_workers(self):
        self.stop_flag.value = True
        for q in self.worker_queues:
            q.put(None)

    # --------------------------------------------------
    def route_chunk(self, chunk):
        worker_chunks = route_chunk_to_workers(chunk, self.num_workers)
        for wid, sub in worker_chunks.items():
            self.worker_queues[wid].put(sub)

    # --------------------------------------------------
    def wait_workers(self):
        for w in self.workers:
            w.join()
        self.workers = []

    # --------------------------------------------------
    def process_file(self, filepath: str) -> Dict[str, Any]:

        start = time.time()

        print("\n=== PASS 1: Parallel Detection ===\n")

        self.start_workers()

        chunk_count = 0

        for chunk in create_mmsi_partitioned_chunks(filepath, self.chunk_size):
            self.route_chunk(chunk)
            chunk_count += 1

        print(f"[Main] Sent {chunk_count} chunks")

        self.stop_workers()

        pass1_results = self.collect_results()
        self.wait_workers()

        pass1_time = time.time() - start
        print(f"[Main] PASS 1 done in {pass1_time:.2f}s")

        # ==========================================================
        # PASS 2 (BATCH LOITERING)
        # ==========================================================
        print("\n=== PASS 2: Loitering (Batch) ===\n")

        t2 = time.time()

        mmsi_records = defaultdict(list)

        for mmsi, ts, epoch, lat, lon, sog, draught in stream_valid_rows(filepath):
            mmsi_records[mmsi].append((ts, epoch, lat, lon, sog, draught))

        for recs in mmsi_records.values():
            recs.sort(key=lambda r: r[1])

        loitering = detect_loitering_anomalies(mmsi_records)

        pass2_time = time.time() - t2

        print(f"[Main] PASS 2 done in {pass2_time:.2f}s")

        # ==========================================================
        # PASS 3
        # ==========================================================
        print("\n=== PASS 3: Scoring ===\n")

        all_anomalies = pass1_results['anomalies'] + loitering

        vessels = aggregate_anomalies_by_vessel(all_anomalies)
        ranked = rank_vessels_by_dfsi(vessels, TOP_N_VESSELS)

        self.save_results(all_anomalies)

        total_time = time.time() - start

        print("\n=== DONE ===")
        print(f"Total time: {total_time:.2f}s")
        print(f"Total anomalies: {len(all_anomalies)}")

        return {
            "pass1_seconds": pass1_time,
            "pass2_seconds": pass2_time,
            "total": total_time,
        }

    # --------------------------------------------------
    def collect_results(self):
        anomalies = []
        total_records = 0
        mmsi_counts = defaultdict(int)

        received = 0

        while received < self.num_workers:
            res = self.result_queue.get()
            anomalies.extend(res['anomalies'])
            total_records += res['total_records']

            for m, c in res['mmsi_counts'].items():
                mmsi_counts[m] += c

            received += 1

        return {
            'anomalies': anomalies,
            'total_records': total_records,
            'mmsi_counts': dict(mmsi_counts),
        }

    # --------------------------------------------------
    def save_results(self, anomalies):

        print("\n[Analysis] Writing CSV...")

        os.makedirs(ANALYSIS_DIR, exist_ok=True)

        with open(os.path.join(ANALYSIS_DIR, "all_anomalies.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=['mmsi', 'anomaly_type'])
            writer.writeheader()

            for a in anomalies:
                writer.writerow({
                    'mmsi': a.get('mmsi', a.get('mmsi_vessel1')),
                    'anomaly_type': a.get('anomaly_type'),
                })


# =====================================================================
# MAIN
# =====================================================================
def main():

    data_dir = "./data"

    files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]

    if not files:
        print("No CSV files found.")
        return

    filepath = os.path.join(data_dir, files[0])

    pipeline = AISPipeline()

    pipeline.process_file(filepath)


if __name__ == "__main__":
    main()
