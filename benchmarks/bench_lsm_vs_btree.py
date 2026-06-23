"""Benchmark: LSM-tree storage (Track C) vs B+ tree / heap-file storage.

Compares the two storage engines on the same workload and reports the three
amplification dimensions the brief asks for:

    * Write throughput   (inserts / second)
    * Read latency        (microseconds / point lookup)
    * Space amplification (bytes on disk / logical data bytes)
    * Write amplification (LSM bytes written / logical data bytes)

Run:  python3 -m benchmarks.bench_lsm_vs_btree [N]
"""

import os
import random
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minidb.lsm.lsm_engine import LSMEngine
from minidb.storage.disk_manager import DiskManager
from minidb.storage.buffer_pool import BufferPool
from minidb.storage.heap_file import HeapFile
from minidb.index.bplus_tree import BPlusTree


def _value(i):
    return f"user_{i}_{'x' * 40}".encode()       # ~50-byte rows


class BTreeStore:
    """Heap file + B+ tree primary index — MiniDB's default storage path."""
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


def run(n=50_000):
    keys = list(range(n))
    logical_bytes = sum(len(_value(k)) for k in keys)
    sample = random.sample(keys, min(5000, n))
    miss_keys = list(range(n, n + 5000))     # all-absent keys (negative lookups)
    results = {}

    for label, factory in (("BTree", BTreeStore),
                           ("LSM", lambda d: LSMEngine(d, memtable_limit=5000))):
        d = tempfile.mkdtemp()
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
        shutil.rmtree(d)
    return n, logical_bytes, results


def _fmt(n, logical_bytes, r):
    lines = [
        f"# LSM vs B+Tree Benchmark (N={n:,} keys, ~{logical_bytes/1e6:.1f} MB logical)",
        "",
        "| Metric | B+Tree (heap) | LSM-tree |",
        "|---|---|---|",
        f"| Write throughput (ops/s) | {r['BTree']['write_throughput']:,.0f} | {r['LSM']['write_throughput']:,.0f} |",
        f"| Point read — hit (µs) | {r['BTree']['read_hit_us']:.2f} | {r['LSM']['read_hit_us']:.2f} |",
        f"| Point read — miss (µs) | {r['BTree']['read_miss_us']:.2f} | {r['LSM']['read_miss_us']:.2f} |",
        f"| Space amplification | {r['BTree']['space_amp']:.2f}x | {r['LSM']['space_amp']:.2f}x |",
        f"| Write amplification | {r['BTree']['write_amp']:.2f}x | {r['LSM']['write_amp']:.2f}x |",
        f"| Compactions | {r['BTree']['compactions']} | {r['LSM']['compactions']} |",
        f"| Bloom-filter skips (5k misses) | {r['BTree']['bloom_skips']} | {r['LSM']['bloom_skips']} |",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 50_000
    random.seed(42)
    table = _fmt(*run(n))
    print(table)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.md")
    with open(out, "w") as f:
        f.write(table + "\n")
    print(f"\n(results written to {out})")
