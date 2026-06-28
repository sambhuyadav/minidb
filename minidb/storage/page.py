"""Slotted-page implementation for heap files.

A slotted page lets us store variable-length records and reclaim space when
records are deleted, while keeping a stable record identifier (the slot number)
even as the physical bytes move during compaction.

Layout (offsets in bytes):
    0..1   num_slots      (unsigned short)  number of slot-dir entries
    2..3   free_ptr       (unsigned short)  start offset of the record area
                                            (records live in [free_ptr, PAGE_SIZE))
    4..    slot directory  -- entry i = (offset:2, length:2)
                              length == 0 means the slot is a tombstone (deleted)

Records are appended growing *downward* from the end of the page; the slot
directory grows *upward* from the header. They meet in the middle; the gap is
free space.
"""

import struct

from ..constants import PAGE_SIZE, PAGE_HEADER_SIZE, SLOT_SIZE

_HEADER = struct.Struct("<HH")   # num_slots, free_ptr
_SLOT = struct.Struct("<HH")     # offset, length


class SlottedPage:
    """A view over a single page's bytes that understands the slotted layout.

    The underlying ``data`` is a ``bytearray`` owned by the buffer pool, so any
    mutation here directly dirties the cached frame.
    """

    def __init__(self, data: bytearray):
        assert len(data) == PAGE_SIZE, "page must be exactly PAGE_SIZE bytes"
        self.data = data

    # --- header helpers -----------------------------------------------------
    @classmethod
    def init_empty(cls, data: bytearray) -> "SlottedPage":
        """Format a fresh (zeroed) page as an empty slotted page."""
        page = cls(data)
        page._set_header(0, PAGE_SIZE)
        return page

    def _header(self):
        return _HEADER.unpack_from(self.data, 0)

    def _set_header(self, num_slots: int, free_ptr: int):
        _HEADER.pack_into(self.data, 0, num_slots, free_ptr)

    @property
    def num_slots(self) -> int:
        return self._header()[0]

    @property
    def free_ptr(self) -> int:
        return self._header()[1]

    # --- slot directory helpers --------------------------------------------
    def _slot(self, i: int):
        return _SLOT.unpack_from(self.data, PAGE_HEADER_SIZE + i * SLOT_SIZE)

    def _set_slot(self, i: int, offset: int, length: int):
        _SLOT.pack_into(self.data, PAGE_HEADER_SIZE + i * SLOT_SIZE, offset, length)

    def free_space(self) -> int:
        """Bytes available for one more (record + slot)."""
        num_slots, free_ptr = self._header()
        slot_dir_end = PAGE_HEADER_SIZE + num_slots * SLOT_SIZE
        return free_ptr - slot_dir_end

    # --- record operations --------------------------------------------------
    def can_insert(self, record: bytes) -> bool:
        return self.free_space() >= len(record) + SLOT_SIZE

    def insert(self, record: bytes) -> int:
        """Insert a record, return its slot number. Caller must check space."""
        num_slots, free_ptr = self._header()
        # First try to reuse a tombstoned slot of sufficient... we keep it simple:
        # always append a new slot (delete just tombstones; vacuum reclaims).
        if self.free_space() < len(record) + SLOT_SIZE:
            raise ValueError("not enough space on page")
        new_free = free_ptr - len(record)
        self.data[new_free:free_ptr] = record
        self._set_slot(num_slots, new_free, len(record))
        self._set_header(num_slots + 1, new_free)
        return num_slots

    def get(self, slot: int):
        """Return the record bytes for ``slot`` or ``None`` if deleted/invalid."""
        if slot < 0 or slot >= self.num_slots:
            return None
        offset, length = self._slot(slot)
        if length == 0:
            return None
        return bytes(self.data[offset:offset + length])

    def update(self, slot: int, record: bytes) -> bool:
        """In-place update if the new record fits the old footprint.

        Returns False if it doesn't fit (caller should delete+insert elsewhere).
        We only allow same-or-smaller to keep slot offsets stable.
        """
        offset, length = self._slot(slot)
        if length == 0 or len(record) > length:
            return False
        self.data[offset:offset + len(record)] = record
        self._set_slot(slot, offset, len(record))
        return True

    def delete(self, slot: int) -> bool:
        """Tombstone a slot. Space is reclaimed only by a later vacuum."""
        if slot < 0 or slot >= self.num_slots:
            return False
        offset, length = self._slot(slot)
        if length == 0:
            return False
        self._set_slot(slot, offset, 0)
        return True

    def items(self):
        """Yield (slot, record_bytes) for all live records."""
        for i in range(self.num_slots):
            rec = self.get(i)
            if rec is not None:
                yield i, rec
