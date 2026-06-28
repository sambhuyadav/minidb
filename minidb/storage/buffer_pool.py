"""Buffer pool - caches a fixed number of pages in memory.

Responsibilities:
  * Serve pages from memory when resident (a buffer hit), else read from disk.
  * Pin pages in use so they are never evicted mid-operation.
  * Track dirty pages and flush them before eviction / on commit / on shutdown.
  * Evict using LRU when all frames are full and a new page is requested.

The replacement policy is LRU over *unpinned* frames, kept in an OrderedDict
where most-recently-used is at the end.
"""

from collections import OrderedDict

from ..constants import BUFFER_POOL_FRAMES
from .disk_manager import DiskManager
from .page import SlottedPage


class Frame:
    __slots__ = ("page_id", "data", "pin_count", "is_dirty")

    def __init__(self, page_id: int, data: bytearray):
        self.page_id = page_id
        self.data = data
        self.pin_count = 0
        self.is_dirty = False


class BufferPool:
    def __init__(self, disk: DiskManager, capacity: int = BUFFER_POOL_FRAMES):
        self.disk = disk
        self.capacity = capacity
        self._frames: "OrderedDict[int, Frame]" = OrderedDict()
        self.hits = 0
        self.misses = 0

    # --- core API -----------------------------------------------------------
    def fetch_page(self, page_id: int) -> Frame:
        """Return a pinned frame for ``page_id`` (reading from disk on a miss)."""
        if page_id in self._frames:
            self.hits += 1
            self._frames.move_to_end(page_id)        # mark as recently used
            frame = self._frames[page_id]
            frame.pin_count += 1
            return frame
        self.misses += 1
        self._evict_if_needed()
        data = self.disk.read_page(page_id)
        frame = Frame(page_id, data)
        frame.pin_count = 1
        self._frames[page_id] = frame
        return frame

    def new_page(self) -> Frame:
        """Allocate a fresh page on disk, format it, and return it pinned."""
        page_id = self.disk.allocate_page()
        self._evict_if_needed()
        data = bytearray(self.disk.read_page(page_id))
        SlottedPage.init_empty(data)
        frame = Frame(page_id, data)
        frame.pin_count = 1
        frame.is_dirty = True
        self._frames[page_id] = frame
        return frame

    def unpin_page(self, page_id: int, is_dirty: bool) -> None:
        frame = self._frames.get(page_id)
        if frame is None:
            return
        if is_dirty:
            frame.is_dirty = True
        if frame.pin_count > 0:
            frame.pin_count -= 1

    def flush_page(self, page_id: int) -> None:
        frame = self._frames.get(page_id)
        if frame and frame.is_dirty:
            self.disk.write_page(page_id, bytes(frame.data))
            frame.is_dirty = False

    def flush_all(self) -> None:
        for pid in list(self._frames.keys()):
            self.flush_page(pid)

    # --- eviction -----------------------------------------------------------
    def _evict_if_needed(self) -> None:
        if len(self._frames) < self.capacity:
            return
        # Find the least-recently-used unpinned frame.
        for pid, frame in self._frames.items():
            if frame.pin_count == 0:
                if frame.is_dirty:
                    self.disk.write_page(pid, bytes(frame.data))
                del self._frames[pid]
                return
        raise RuntimeError("buffer pool full and all frames are pinned")

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_ratio": (self.hits / total) if total else 0.0,
            "resident": len(self._frames),
            "capacity": self.capacity,
        }
