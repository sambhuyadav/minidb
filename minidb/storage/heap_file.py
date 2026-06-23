"""Heap file — an unordered collection of slotted pages holding one table's rows.

A record is addressed by a RID = (page_id, slot). The set of page ids belonging
to the table is owned/persisted by the catalog and passed in here, so the heap
file itself stays stateless across restarts apart from that page list.
"""

from .buffer_pool import BufferPool
from .page import SlottedPage

# A RID is just a (page_id, slot) tuple. Defined as a name for readability.
RID = tuple


class HeapFile:
    def __init__(self, buffer_pool: BufferPool, page_ids: list):
        self.bp = buffer_pool
        self.page_ids = page_ids          # mutable list, shared with catalog

    def insert(self, record: bytes) -> RID:
        """Insert into the last page with room, else grow the file by a page."""
        # Try existing pages from the end first (better locality for appends).
        for page_id in reversed(self.page_ids):
            frame = self.bp.fetch_page(page_id)
            page = SlottedPage(frame.data)
            if page.can_insert(record):
                slot = page.insert(record)
                self.bp.unpin_page(page_id, is_dirty=True)
                return (page_id, slot)
            self.bp.unpin_page(page_id, is_dirty=False)
        # No room anywhere -> allocate a new page.
        frame = self.bp.new_page()
        page = SlottedPage(frame.data)
        slot = page.insert(record)
        self.page_ids.append(frame.page_id)
        self.bp.unpin_page(frame.page_id, is_dirty=True)
        return (frame.page_id, slot)

    def get(self, rid: RID):
        page_id, slot = rid
        frame = self.bp.fetch_page(page_id)
        try:
            return SlottedPage(frame.data).get(slot)
        finally:
            self.bp.unpin_page(page_id, is_dirty=False)

    def update(self, rid: RID, record: bytes) -> RID:
        """Update in place if it fits; otherwise delete + reinsert (new RID)."""
        page_id, slot = rid
        frame = self.bp.fetch_page(page_id)
        page = SlottedPage(frame.data)
        if page.update(slot, record):
            self.bp.unpin_page(page_id, is_dirty=True)
            return rid
        page.delete(slot)
        self.bp.unpin_page(page_id, is_dirty=True)
        return self.insert(record)

    def delete(self, rid: RID) -> bool:
        page_id, slot = rid
        frame = self.bp.fetch_page(page_id)
        ok = SlottedPage(frame.data).delete(slot)
        self.bp.unpin_page(page_id, is_dirty=ok)
        return ok

    def scan(self):
        """Yield (rid, record_bytes) over all live records in the table."""
        for page_id in self.page_ids:
            frame = self.bp.fetch_page(page_id)
            page = SlottedPage(frame.data)
            records = list(page.items())     # snapshot before unpinning
            self.bp.unpin_page(page_id, is_dirty=False)
            for slot, rec in records:
                yield (page_id, slot), rec
