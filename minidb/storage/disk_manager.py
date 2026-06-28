"""Disk manager - owns the on-disk file and does raw page-grained I/O.

This is the only layer that issues real ``read``/``write`` syscalls. Everything
above it speaks in page ids; the disk manager translates a page id into a byte
offset (page_id * PAGE_SIZE) in the data file.
"""

import os

from ..constants import PAGE_SIZE


class DiskManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        # "r+b" needs the file to exist; create it first if missing.
        if not os.path.exists(db_path):
            open(db_path, "wb").close()
        self._f = open(db_path, "r+b")
        self._f.seek(0, os.SEEK_END)
        self._num_pages = self._f.tell() // PAGE_SIZE
        self.reads = 0    # counters for benchmarking / observability
        self.writes = 0

    @property
    def num_pages(self) -> int:
        return self._num_pages

    def allocate_page(self) -> int:
        """Grow the file by one zeroed page and return its page id."""
        page_id = self._num_pages
        self._f.seek(page_id * PAGE_SIZE)
        self._f.write(bytes(PAGE_SIZE))
        self._f.flush()
        self._num_pages += 1
        return page_id

    def read_page(self, page_id: int) -> bytearray:
        if page_id < 0 or page_id >= self._num_pages:
            raise IndexError(f"page {page_id} out of range (have {self._num_pages})")
        self._f.seek(page_id * PAGE_SIZE)
        data = self._f.read(PAGE_SIZE)
        if len(data) < PAGE_SIZE:           # short read at EOF -> pad
            data = data + bytes(PAGE_SIZE - len(data))
        self.reads += 1
        return bytearray(data)

    def write_page(self, page_id: int, data: bytes) -> None:
        assert len(data) == PAGE_SIZE
        self._f.seek(page_id * PAGE_SIZE)
        self._f.write(data)
        self._f.flush()
        os.fsync(self._f.fileno())          # durability: force to stable storage
        self.writes += 1

    def close(self) -> None:
        try:
            self._f.flush()
            self._f.close()
        except Exception:
            pass
