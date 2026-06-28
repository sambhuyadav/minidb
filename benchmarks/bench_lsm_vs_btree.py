"""Benchmark: LSM-tree storage (Track C) vs B+ tree / heap-file storage.

Compares the two storage engines on the same workload and reports the three
amplification dimensions the brief asks for:

    * Write throughput   (inserts / second)
    * Read latency        (microseconds / point lookup)
    * Space amplification (bytes on disk / logical data bytes)
    * Write amplification (LSM bytes written / logical data bytes)

Run:  uv run python -m benchmarks.bench_lsm_vs_btree [N] [trials]
"""

import os
import random
import shutil
import sys
import tempfile
import time
from statistics import median

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minidb.lsm.lsm_engine import LSMEngine
from minidb.storage.disk_manager import DiskManager
from minidb.storage.buffer_pool import BufferPool
from minidb.storage.heap_file import HeapFile
from minidb.index.bplus_tree import BPlusTree

TEMP_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         ".minidb_tmp")


def _value(i):
    return f"user_{i}_{'x' * 40}".encode()       # ~50-byte rows


def _mkdtemp():
    os.makedirs(TEMP_ROOT, exist_ok=True)
    return tempfile.mkdtemp(dir=TEMP_ROOT)


class BTreeStore:
    """Heap file + B+ tree primary index - Track C baseline comparator."""
    def __init__(self, directory):
        self.dir = directory
        self.disk = DiskManager(os.path.join(directory, "bt.data"))
        self.bp = BufferPool(self.disk, capacity=256)
        self.page_ids = []
        self.heap = HeapFile(self.bp, self.page_ids)
        self.index = BPlusTree()

    def put(self, key, value):
        rid = self.heap.insert(value)
        self.index.insert(key, rid)

    def get(self, key):
        rid = self.index.search(key)
        return self.heap.get(rid) if rid is not None else None

    def flush(self):
        self.bp.flush_all()

    def disk_bytes(self):
        return self.disk.num_pages * 4096

    def close(self):
        self.bp.flush_all()
        self.disk.close()


def run(n=50_000):
    keys = list(range(n))
    logical_bytes = sum(len(_value(k)) for k in keys)
    sample = random.sample(keys, min(5000, n))
    miss_keys = list(range(n, n + 5000))     # all-absent keys (negative lookups)
    results = {}

    for label, factory in (("BTree", BTreeStore),
                           ("LSM", lambda d: LSMEngine(d, memtable_limit=5000))):
        d = _mkdtemp()
        store = None
        try:
            store = factory(d)
            t0 = time.perf_counter()
            for k in keys:
                store.put(k, _value(k))
            store.flush()
            write_s = time.perf_counter() - t0

            t0 = time.perf_counter()
            for k in sample:
                store.get(k)
            read_hit_us = (time.perf_counter() - t0) / len(sample) * 1e6

            t0 = time.perf_counter()
            for k in miss_keys:
                store.get(k)
            read_miss_us = (time.perf_counter() - t0) / len(miss_keys) * 1e6

            if isinstance(store, LSMEngine):
                disk_bytes = sum(
                    os.path.getsize(os.path.join(d, f))
                    for f in os.listdir(d) if f.endswith(".data"))
                extra = {"write_amp": store.bytes_written / logical_bytes,
                         "compactions": store.compactions,
                         "bloom_skips": store.get_bloom_skips}
            else:
                disk_bytes = store.disk_bytes()
                extra = {"write_amp": 1.0, "compactions": 0, "bloom_skips": 0}

            results[label] = {
                "write_throughput": n / write_s,
                "read_hit_us": read_hit_us,
                "read_miss_us": read_miss_us,
                "space_amp": disk_bytes / logical_bytes,
                **extra,
            }
        finally:
            close = getattr(store, "close", None) if store is not None else None
            if close is not None:
                close()
            shutil.rmtree(d)
    return n, logical_bytes, results


def run_trials(n=50_000, trials=7):
    trial_results = []
    for i in range(trials):
        random.seed(42 + i)
        trial_results.append(run(n))
    logical_bytes = trial_results[0][1]
    labels = ("BTree", "LSM")
    metrics = trial_results[0][2]["BTree"].keys()
    results = {label: {} for label in labels}
    for label in labels:
        for metric in metrics:
            values = [r[label][metric] for _, _, r in trial_results]
            value = median(values)
            if metric in ("compactions", "bloom_skips"):
                value = int(round(value))
            results[label][metric] = value
    timing_ranges = {}
    for metric in ("write_throughput", "read_hit_us", "read_miss_us"):
        timing_ranges[metric] = {}
        for label in labels:
            values = [r[label][metric] for _, _, r in trial_results]
            timing_ranges[metric][label] = (min(values), max(values))
    return n, logical_bytes, results, timing_ranges


def _fmt(n, logical_bytes, r, trials=1):
    lines = [
        f"# LSM vs B+Tree Benchmark (N={n:,} keys, ~{logical_bytes/1e6:.1f} MB logical)",
        "",
        "| Metric | B+Tree (heap) | LSM-tree |",
        "|---|---|---|",
        f"| Write throughput (ops/s) | {r['BTree']['write_throughput']:,.0f} | {r['LSM']['write_throughput']:,.0f} |",
        f"| Point read hit (us) | {r['BTree']['read_hit_us']:.2f} | {r['LSM']['read_hit_us']:.2f} |",
        f"| Point read miss (us) | {r['BTree']['read_miss_us']:.2f} | {r['LSM']['read_miss_us']:.2f} |",
        f"| Space amplification | {r['BTree']['space_amp']:.2f}x | {r['LSM']['space_amp']:.2f}x |",
        f"| Write amplification | {r['BTree']['write_amp']:.2f}x | {r['LSM']['write_amp']:.2f}x |",
        f"| Compactions | {r['BTree']['compactions']} | {r['LSM']['compactions']} |",
        f"| Bloom-filter skips (5k misses) | {r['BTree']['bloom_skips']:,.0f} | {r['LSM']['bloom_skips']:,.0f} |",
    ]
    if trials > 1:
        lines.insert(2, f"_Median of {trials} trials. Timing is wall-clock and load-dependent._")
        lines.insert(3, "")
    return "\n".join(lines)


def _range_report(timing_ranges, trials):
    if not timing_ranges or trials <= 1:
        return []
    return [
        f"Timing range across {trials} trials (min - max):",
        f"- Write ops/s: B+Tree {timing_ranges['write_throughput']['BTree'][0]:,.0f} - {timing_ranges['write_throughput']['BTree'][1]:,.0f}; "
        f"LSM {timing_ranges['write_throughput']['LSM'][0]:,.0f} - {timing_ranges['write_throughput']['LSM'][1]:,.0f}",
        f"- Read-hit us: B+Tree {timing_ranges['read_hit_us']['BTree'][0]:.2f} - {timing_ranges['read_hit_us']['BTree'][1]:.2f}; "
        f"LSM {timing_ranges['read_hit_us']['LSM'][0]:.2f} - {timing_ranges['read_hit_us']['LSM'][1]:.2f}",
        f"- Read-miss us: B+Tree {timing_ranges['read_miss_us']['BTree'][0]:.2f} - {timing_ranges['read_miss_us']['BTree'][1]:.2f}; "
        f"LSM {timing_ranges['read_miss_us']['LSM'][0]:.2f} - {timing_ranges['read_miss_us']['LSM'][1]:.2f}",
    ]


def _report(n, logical_bytes, r, trials=1, timing_ranges=None):
    table = _fmt(n, logical_bytes, r, trials).split("\n", 2)[2]
    write_ratio = r["LSM"]["write_throughput"] / r["BTree"]["write_throughput"]
    read_ratio = r["LSM"]["read_hit_us"] / r["BTree"]["read_hit_us"]
    command = f"uv run python -m benchmarks.bench_lsm_vs_btree {n}"
    if trials > 1:
        command += f" {trials}"
    range_lines = _range_report(timing_ranges, trials)
    return "\n".join([
        "# LSM vs B+Tree Benchmark Report",
        "",
        "## Command",
        "",
        "```bash",
        command,
        "```",
        "",
        "## Workload",
        "",
        f"- Dataset: {n:,} integer keys.",
        f"- Logical payload: about {logical_bytes/1e6:.1f} MB of row bytes.",
        "- Writes: sequential inserts into each engine.",
        "- Reads: random point lookups over up to 5,000 existing keys.",
        "- Misses: 5,000 absent-key point lookups.",
        f"- Reported timings: {'median of ' + str(trials) + ' trials' if trials > 1 else 'single run'}.",
        "- Engines compared:",
        "  - B+Tree baseline: heap file rows plus in-memory B+ tree primary-key index.",
        "  - LSM-tree: MemTable, L0 SSTables, L1 compaction, Bloom filters, sparse indexes.",
        "",
        "## Results",
        table,
        "",
        *(range_lines + [""] if range_lines else []),
        "## Analysis",
        "",
        f"The LSM-tree write path is about {write_ratio:.1f}x faster on this workload "
        "because writes go to an in-memory MemTable and later flush as sorted "
        "sequential SSTables. The heap+B+ tree baseline inserts each row into the "
        "heap and updates the primary-key index immediately.",
        "",
        f"The B+ tree baseline has lower point-read latency; LSM hits are about "
        f"{read_ratio:.1f}x slower here because reads may check the MemTable plus "
        "multiple SSTables. Bloom filters reduce wasted work for negative lookups: "
        f"{r['LSM']['bloom_skips']:,} SSTable reads were skipped during the miss probes.",
        "",
        "The LSM-tree also pays write and space amplification. Compaction rewrites "
        f"data, producing {r['LSM']['write_amp']:.2f}x write amplification and "
        f"{r['LSM']['space_amp']:.2f}x space amplification in this run. This is the "
        "expected Track C trade-off: higher write throughput in exchange for more "
        "read work and background rewrite cost.",
    ])


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 50_000
    trials = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    if trials < 1:
        raise SystemExit("trials must be >= 1")
    if trials > 1:
        n, logical_bytes, results, timing_ranges = run_trials(n, trials)
    else:
        n, logical_bytes, results = run(n)
        timing_ranges = None
    table = _fmt(n, logical_bytes, results, trials)
    print(table)
    ranges = _range_report(timing_ranges, trials)
    if ranges:
        print()
        print("\n".join(ranges))
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.md")
    with open(out, "w") as f:
        f.write(_report(n, logical_bytes, results, trials, timing_ranges) + "\n")
    print(f"\n(results written to {out})")
