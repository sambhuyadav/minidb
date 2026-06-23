"""Sorted String Table (SSTable) — an immutable, sorted, on-disk run.

Once written, an SSTable is never modified; updates and deletes are handled by
writing *newer* runs and merging during compaction. Each SSTable persists:

  * ``.data`` : entries sorted by key, encoded as
                [keylen:4][key][flag:1][vallen:4][value]
                flag 1 == tombstone (deletion marker), value is empty.
  * ``.meta`` : pickled (sparse_index, bloom, count, min_key, max_key)
                sparse_index = list of (key, byte_offset) every SPARSE_STEP keys
                so a lookup binary-searches the index then scans one short block.
"""

import os
import pickle
import struct

from .bloom import BloomFilter

TOMBSTONE = b"\x00__MINIDB_TOMBSTONE__"   # sentinel value meaning "deleted"
SPARSE_STEP = 16                          # index every 16th key

_KLEN = struct.Struct("<I")
_FLAG = struct.Struct("<B")
_VLEN = struct.Struct("<I")


class SSTable:
    def __init__(self, path_prefix: str):
        self.prefix = path_prefix
        self.data_path = path_prefix + ".data"
        self.meta_path = path_prefix + ".meta"
        self.sparse_index = []
        self.bloom = None
        self.count = 0
        self.min_key = None
        self.max_key = None
        self.size_bytes = 0
        self.bloom_checks = 0
        self.bloom_skips = 0

    # --- writing ------------------------------------------------------------
    @classmethod
    def write(cls, path_prefix: str, sorted_items) -> "SSTable":
        """Write a list of (key:bytes, value:bytes|TOMBSTONE) already sorted by key."""
        items = list(sorted_items)
        sst = cls(path_prefix)
        sst.bloom = BloomFilter(len(items))
        with open(sst.data_path, "wb") as f:
            for i, (key, value) in enumerate(items):
                offset = f.tell()
                is_tomb = value is TOMBSTONE
                val = b"" if is_tomb else value
                f.write(_KLEN.pack(len(key)))
                f.write(key)
                f.write(_FLAG.pack(1 if is_tomb else 0))
                f.write(_VLEN.pack(len(val)))
                f.write(val)
                sst.bloom.add(key)
                if i % SPARSE_STEP == 0:
                    sst.sparse_index.append((key, offset))
            f.flush()
            os.fsync(f.fileno())
        sst.count = len(items)
        sst.min_key = items[0][0] if items else None
        sst.max_key = items[-1][0] if items else None
        sst.size_bytes = os.path.getsize(sst.data_path)
        with open(sst.meta_path, "wb") as f:
            pickle.dump({
                "sparse_index": sst.sparse_index,
                "bloom": sst.bloom.to_dict(),
                "count": sst.count,
                "min_key": sst.min_key,
                "max_key": sst.max_key,
            }, f)
        return sst

    @classmethod
    def open(cls, path_prefix: str) -> "SSTable":
        sst = cls(path_prefix)
        with open(sst.meta_path, "rb") as f:
            m = pickle.load(f)
        sst.sparse_index = m["sparse_index"]
        sst.bloom = BloomFilter.from_dict(m["bloom"])
        sst.count = m["count"]
        sst.min_key = m["min_key"]
        sst.max_key = m["max_key"]
        sst.size_bytes = os.path.getsize(sst.data_path)
        return sst

    # --- reading ------------------------------------------------------------
    def get(self, key: bytes):
        """Return (found, value_or_None). value None == tombstone (deleted)."""
        self.bloom_checks += 1
        if not self.bloom.maybe_contains(key):
            self.bloom_skips += 1
            return (False, None)              # Bloom filter saved a disk read
        if self.min_key is not None and (key < self.min_key or key > self.max_key):
            return (False, None)
        # Binary search the sparse index for the block to scan.
        lo, hi, start = 0, len(self.sparse_index) - 1, 0
        while lo <= hi:
            mid = (lo + hi) // 2
            if self.sparse_index[mid][0] <= key:
                start = self.sparse_index[mid][1]
                lo = mid + 1
            else:
                hi = mid - 1
        with open(self.data_path, "rb") as f:
            f.seek(start)
            while True:
                head = f.read(_KLEN.size)
                if not head:
                    break
                (klen,) = _KLEN.unpack(head)
                k = f.read(klen)
                (flag,) = _FLAG.unpack(f.read(_FLAG.size))
                (vlen,) = _VLEN.unpack(f.read(_VLEN.size))
                v = f.read(vlen)
                if k == key:
                    return (True, None if flag == 1 else v)
                if k > key:
                    break                     # passed it; sorted run
        return (False, None)

    def scan(self):
        """Yield (key, value_or_TOMBSTONE) in sorted order (used by compaction)."""
        with open(self.data_path, "rb") as f:
            while True:
                head = f.read(_KLEN.size)
                if not head:
                    return
                (klen,) = _KLEN.unpack(head)
                k = f.read(klen)
                (flag,) = _FLAG.unpack(f.read(_FLAG.size))
                (vlen,) = _VLEN.unpack(f.read(_VLEN.size))
                v = f.read(vlen)
                yield k, (TOMBSTONE if flag == 1 else v)

    def remove_files(self):
        for p in (self.data_path, self.meta_path):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
