# task1.py
"""
Command-line interface for Two-Pass Shadow Fleet Detection.
Pass 1: Parallel detection (A, C, D)
Pass 2: Loitering detection (B) - STREAMING VERSION
"""
import csv
from analysis import write_anomalies_csv, write_vessel_scores_csv, write_metadata_json
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
from loiter import detect_loitering_anomalies
from scoring import (
    calculate_dfsi,
    aggregate_anomalies_by_vessel,
    rank_vessels_by_dfsi,
)
from parsing import stream_csv_rows, is_valid_mmsi, stream_valid_rows
from geo import is_valid_coordinate


def get_memory_usage_mb() -> float:
    """Get current memory usage in MB."""
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_maxrss / (1024 * 1024)
    except Exception:
        return 0.0


def worker_process(
    worker_id: int,
    task_queue: Queue,
    result_queue: Queue,
    stop_flag: Synchronized,
) -> None:
    """
    Worker process: PASS 1 - Detect anomalies A, C, D.
    """
    processed_chunks = 0
    total_records = 0
    mmsi_data: Dict[str, List[Tuple]] = defaultdict(list)

    print(f"[Worker {worker_id}] Started (Pass 1: Detect A, C, D)")

    # Phase 1: Accumulate records
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
                mem_mb = get_memory_usage_mb()
                print(
                    f"[Worker {worker_id}] {processed_chunks} chunks, "
                    f"{total_records:,} records, {len(mmsi_data)} vessels, Mem: {mem_mb:.1f}MB"
                )

        except Exception as e:
            if "Empty" not in str(type(e).__name__):
                print(f"[Worker {worker_id}] Error: {e}")
            continue

    # Phase 2: Detect A, C, D anomalies
    print(f"[Worker {worker_id}] Starting anomaly detection...")
    going_dark_anomalies = []
    teleportation_anomalies = []
    draft_change_anomalies = []
    
    for mmsi, records in mmsi_data.items():
        if len(records) < 2:
            continue
        records.sort(key=lambda r: r[1])
        
        going_dark_anomalies.extend(detect_going_dark_anomalies(mmsi, records))
        teleportation_anomalies.extend(detect_teleportation_anomalies(mmsi, records))
        draft_change_anomalies.extend(detect_draft_change_anomalies(mmsi, records))
    
    all_anomalies = going_dark_anomalies + teleportation_anomalies + draft_change_anomalies

    result_queue.put({
        'worker_id': worker_id,
        'processed_chunks': processed_chunks,
        'total_records': total_records,
        'mmsi_counts': {mmsi: len(recs) for mmsi, recs in mmsi_data.items()},
        'anomalies': all_anomalies,
        'going_dark': going_dark_anomalies,
        'teleportation': teleportation_anomalies,
        'draft_change': draft_change_anomalies,
        'final_memory_mb': get_memory_usage_mb(),
    })
    
    print(
        f"[Worker {worker_id}] Finished — {total_records:,} records, "
        f"{len(all_anomalies)} total anomalies "
        f"(A: {len(going_dark_anomalies)}, D: {len(teleportation_anomalies)}, C: {len(draft_change_anomalies)})"
    )


class AISPipeline:
    """Main orchestration pipeline with TWO-PASS processing."""

    def __init__(self, num_workers: int = NUM_WORKERS, chunk_size: int = CHUNK_SIZE):
        self.num_workers = num_workers
        self.chunk_size = chunk_size
        self.worker_queues: List[Queue] = [Queue(maxsize=8) for _ in range(num_workers)]
        self.result_queue: Queue = Queue()
        self.stop_flag = Value('b', False)
        self.workers: List[Process] = []
        
        # Create output directories
        for dir_path in OUTPUT_DIRS:
            os.makedirs(dir_path, exist_ok=True)

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

    def _route_chunk(self, chunk: Dict) -> None:
        """Route chunk to workers by MMSI hash."""
        worker_chunks = route_chunk_to_workers(chunk, self.num_workers)
        for worker_id, sub_chunk in worker_chunks.items():
            self.worker_queues[worker_id].put(sub_chunk)

    def wait_for_workers(self) -> None:
        """Wait for all workers to finish."""
        for worker in self.workers:
            worker.join(timeout=180)
            if worker.is_alive():
                worker.terminate()
        self.workers = []

    def process_file(self, filepath: str) -> Dict[str, Any]:
        """
        Process a CSV file through the TWO-PASS pipeline.
        """
        start_time = time.time()
        file_size_gb = os.path.getsize(filepath) / (1024**3)
        
        print(f"\n{'='*70}")
        print(f"Processing: {os.path.basename(filepath)}")
        print(f"File size: {file_size_gb:.2f} GB")
        print(f"Workers: {self.num_workers}")
        print(f"{'='*70}\n")
        
        # ====================================================================
        # PASS 1: Parallel Detection of Anomalies A, C, D
        # ====================================================================
        print(f"{'='*70}")
        print("PASS 1: Parallel Detection (Anomalies A, C, D)")
        print(f"{'='*70}\n")
        
        pass1_start = time.time()
        self.start_workers()
        
        chunks_sent = 0
        try:
            chunk_generator = create_mmsi_partitioned_chunks(filepath, self.chunk_size)
            
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
        pass1_results = self._aggregate_results()
        self.wait_for_workers()
        
        pass1_time = time.time() - pass1_start
        print(f"\n[Main] PASS 1 completed in {pass1_time:.2f} seconds")
        
        # ====================================================================
        # PASS 2: Detect Anomaly B (Loitering) - STREAMING VERSION
        # ====================================================================
        print(f"\n{'='*70}")
        print("PASS 2: Loitering Detection (Anomaly B)")
        print(f"{'='*70}\n")
        
        pass2_start = time.time()
        
        print(f"[Main] Building MMSI records for loitering (batch mode)...")

        mmsi_records = defaultdict(list)

        for mmsi, ts_str, epoch, lat, lon, sog, draught in stream_valid_rows(filepath):
            mmsi_records[mmsi].append((ts_str, epoch, lat, lon, sog, draught))

        # IMPORTANT: sort once per vessel
        for recs in mmsi_records.values():
            recs.sort(key=lambda r: r[1])

        print(f"[Main] Built {len(mmsi_records)} vessels")

        loitering_anomalies = detect_loitering_anomalies(mmsi_records)
        
        pass2_time = time.time() - pass2_start
        print(f"\n[Main] PASS 2 completed in {pass2_time:.2f} seconds")
        
        # ====================================================================
        # PASS 3: Aggregate and Score
        # ====================================================================
        print(f"\n{'='*70}")
        print("PASS 3: Scoring & Ranking (DFSI)")
        print(f"{'='*70}\n")
        
        all_anomalies = pass1_results.get('anomalies', []) + loitering_anomalies
        vessels_dict = aggregate_anomalies_by_vessel(all_anomalies)
        top_vessels = rank_vessels_by_dfsi(vessels_dict, top_n=TOP_N_VESSELS)
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Prepare final results
        final_results = {
            'file': filepath,
            'file_size_gb': file_size_gb,
            'elapsed_seconds': elapsed,
            'pass1_seconds': pass1_time,
            'pass2_seconds': pass2_time,
            'chunks_processed': chunks_sent,
            'throughput_mb_per_sec': (file_size_gb * 1024) / elapsed if elapsed > 0 else 0,
            'total_records': pass1_results['total_records'],
            'unique_vessels': pass1_results['unique_vessels'],
            'mmsi_counts': pass1_results['mmsi_counts'],
            'anomalies': all_anomalies,
            'vessels_by_dfsi': top_vessels,
            'total_flagged_vessels': len(vessels_dict),
            'max_memory_mb': pass1_results['max_memory_mb'],
        }
        
        all_dfsi_scores = [v['dfsi'] for v in top_vessels]
        final_results['dfsi_stats'] = {
            'mean': round(sum(all_dfsi_scores) / len(all_dfsi_scores), 2) if all_dfsi_scores else 0,
            'max': max(all_dfsi_scores) if all_dfsi_scores else 0,
            'min': min(all_dfsi_scores) if all_dfsi_scores else 0,
        }
        
        self._save_results(final_results)
        self._print_summary(final_results)
        
        return final_results

    def _aggregate_results(self) -> Dict[str, Any]:
        """Collect results from all workers - WITHOUT storing mmsi_records."""
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
                    f"{len(result.get('anomalies', []))} anomalies "
                    f"(A: {len(result.get('going_dark', []))}, "
                    f"D: {len(result.get('teleportation', []))}, "
                    f"C: {len(result.get('draft_change', []))})"
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

    def _save_results(self, results: Dict[str, Any]) -> None:
        """Save results to CSV and JSON files."""
        print(f"\n{'='*70}")
        print("WRITING OUTPUT FILES")
        print(f"{'='*70}\n")
        
        all_anomalies = results.get('anomalies', [])
        
        # Write anomalies by type
        print("[Analysis] Writing anomaly events...")
        write_anomalies_csv(all_anomalies, os.path.join(ANALYSIS_DIR, 'anomaly_events.csv'))
        
        # Write combined anomalies CSV
        with open(os.path.join(ANALYSIS_DIR, 'all_anomalies.csv'), 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['mmsi', 'anomaly_type', 'timestamp'])
            writer.writeheader()
            for a in all_anomalies:
                writer.writerow({
                    'mmsi': a.get('mmsi', a.get('mmsi_vessel1', '')),
                    'anomaly_type': a.get('anomaly_type'),
                    'timestamp': a.get('gap_start', a.get('loitering_start', '')),
                })
        print(f"  ✓ Wrote {len(all_anomalies)} combined anomalies to all_anomalies.csv")
        
        # Write vessel scores
        print("\n[Scoring] Writing vessel scores...")
        top_vessels = results.get('vessels_by_dfsi', [])
        write_vessel_scores_csv(top_vessels, os.path.join(ANALYSIS_DIR, 'vessel_scores.csv'))
        
        # Write top 5 as JSON
        with open(os.path.join(ANALYSIS_DIR, 'top5_suspects.json'), 'w') as f:
            json.dump(top_vessels[:5], f, indent=2, default=str)
        print(f"  ✓ Wrote top 5 suspects to top5_suspects.json")
        
        # Write metadata
        print("\n[Pipeline] Writing metadata...")
        metadata = {
            'file': os.path.basename(results['file']),
            'file_size_gb': round(results['file_size_gb'], 2),
            'total_records': results['total_records'],
            'unique_vessels': results['unique_vessels'],
            'anomalies': {
                'total': len(all_anomalies),
                'going_dark': len([a for a in all_anomalies if a.get('anomaly_type') == 'going_dark']),
                'teleportation': len([a for a in all_anomalies if a.get('anomaly_type') == 'teleportation']),
                'draft_change': len([a for a in all_anomalies if a.get('anomaly_type') == 'draft_change']),
                'loitering': len([a for a in all_anomalies if a.get('anomaly_type') == 'loitering']),
            },
            'timing': {
                'pass1_sec': round(results['pass1_seconds'], 2),
                'pass2_sec': round(results['pass2_seconds'], 2),
                'total_sec': round(results['elapsed_seconds'], 2),
                'throughput_mb_sec': round(results['throughput_mb_per_sec'], 2),
            },
            'resources': {
                'peak_memory_mb': round(results['max_memory_mb'], 1),
                'workers': len(results['worker_results']) if 'worker_results' in results else 0,
            },
            'dfsi': {
                'mean': results['dfsi_stats']['mean'],
                'max': results['dfsi_stats']['max'],
                'min': results['dfsi_stats']['min'],
                'flagged_vessels': results.get('total_flagged_vessels', 0),
            },
        }
        write_metadata_json(metadata, os.path.join(ANALYSIS_DIR, 'run_metadata.json'))
        
        print(f"\n{'='*70}")
        print(f"✅ ALL OUTPUTS WRITTEN:")
        print(f"  - {ANALYSIS_DIR}/")
        print(f"  - {OUTPUT_DIRS[2]}/")
        print(f"{'='*70}\n")

    def _print_summary(self, results: Dict[str, Any]) -> None:
        """Print summary of processing results."""
        print(f"\n{'='*70}")
        print("FINAL PROCESSING SUMMARY")
        print(f"{'='*70}")
        print(f"File: {os.path.basename(results['file'])}")
        print(f"File size: {results['file_size_gb']:.2f} GB")
        print(f"Total records: {results['total_records']:,}")
        print(f"Unique vessels: {results['unique_vessels']:,}")
        print(f"Chunks processed: {results['chunks_processed']:,}")
        print()
        print(f"Pass 1 (A, C, D detection): {results['pass1_seconds']:.2f} sec")
        print(f"Pass 2 (B detection):        {results['pass2_seconds']:.2f} sec")
        print(f"Total elapsed time:          {results['elapsed_seconds']:.2f} sec")
        print()
        print(f"Throughput: {results['throughput_mb_per_sec']:.2f} MB/sec")
        print(f"Peak memory: {results['max_memory_mb']:.1f} MB")
        print(f"{'='*70}")

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

        anomalies = results.get('anomalies', [])
        going_dark = [a for a in anomalies if a.get('anomaly_type') == 'going_dark']
        teleportation = [a for a in anomalies if a.get('anomaly_type') == 'teleportation']
        draft_change = [a for a in anomalies if a.get('anomaly_type') == 'draft_change']
        loitering = [a for a in anomalies if a.get('anomaly_type') == 'loitering']
        
        print(f"\n{'='*70}")
        print("ANOMALIES DETECTED")
        print(f"{'='*70}")
        print(f"Anomaly A (Going Dark):      {len(going_dark):>6}")
        print(f"Anomaly D (Teleportation):   {len(teleportation):>6}")
        print(f"Anomaly C (Draft Change):    {len(draft_change):>6}")
        print(f"Anomaly B (Loitering):       {len(loitering):>6}")
        print(f"{'─'*30}")
        print(f"Total Anomalies:             {len(anomalies):>6}")

        if going_dark:
            top_anomalies = sorted(going_dark, key=lambda a: a['gap_hours'], reverse=True)[:TOP_N_GOING_DARK]
            print(f"\nTop {TOP_N_GOING_DARK} Going-Dark Events:")
            print("-" * 60)
            print(f"  {'MMSI':<12} {'Gap (h)':>8}  {'Distance (km)':>14}  Gap window")
            print("-" * 60)
            for a in top_anomalies:
                print(
                    f"  {a['mmsi']:<12} {a['gap_hours']:>8.1f}  "
                    f"{a['distance_km']:>14.1f}  "
                    f"{a['gap_start']} → {a['gap_end']}"
                )

        if 'vessels_by_dfsi' in results:
            top_vessels = results['vessels_by_dfsi'][:10]
            print(f"\n{'='*70}")
            print("TOP 10 SHADOW FLEET SUSPECTS (by DFSI)")
            print(f"{'='*70}")
            print(f"{'Rank':<6} {'MMSI':<12} {'DFSI':>8} {'A':>4} {'D':>6} {'C':>4} {'B':>4}")
            print("-" * 70)
            for i, vessel in enumerate(top_vessels, 1):
                counts = vessel['anomaly_counts']
                print(
                    f"{i:<6} {vessel['mmsi']:<12} {vessel['dfsi']:>8.2f} "
                    f"{counts.get('going_dark', 0):>4} "
                    f"{counts.get('teleportation', 0):>6} "
                    f"{counts.get('draft_change', 0):>4} "
                    f"{counts.get('loitering', 0):>4}"
                )
            
            print(f"\nDFSI Statistics (Top {TOP_N_VESSELS}):")
            print(f"  Mean: {results['dfsi_stats']['mean']:.2f} | "
                  f"Max: {results['dfsi_stats']['max']:.2f} | "
                  f"Min: {results['dfsi_stats']['min']:.2f}")
            print(f"  Total flagged vessels: {results.get('total_flagged_vessels', 0)}")


def main():
    """Main entry point for Task 1."""
    
    DATA_DIR = "./data"
    
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
    print("TWO-PASS SHADOW FLEET DETECTION PIPELINE")
    print("Pass 1: Parallel detection (A, C, D)")
    print("Pass 2: Loitering detection (B) - STREAMING MODE")
    print("="*70)
    
    for csv_file in csv_files:
        if not csv_file.startswith('aisdk-'):
            continue
            
        filepath = os.path.join(DATA_DIR, csv_file)
        
        pipeline = AISPipeline(num_workers=NUM_WORKERS, chunk_size=CHUNK_SIZE)
        results = pipeline.process_file(filepath)
        
        print(f"\n✅ Completed processing {csv_file}")


if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()
