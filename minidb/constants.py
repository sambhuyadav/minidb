"""System-wide constants for MiniDB."""

# A page is the atomic unit of disk I/O. 4 KB matches the OS page size on most
# systems, so one logical page maps to one kernel page (no alignment waste).
PAGE_SIZE = 4096

# Number of frames the buffer pool keeps resident in memory. Deliberately small
# so eviction/replacement is easy to demonstrate during the viva.
BUFFER_POOL_FRAMES = 64

# Sentinel for "no page".
INVALID_PAGE_ID = -1

# Heap page layout constants (slotted page).
#   [ num_slots:2 | free_ptr:2 | slot_dir... | ........ free ........ | records ]
# Slot directory grows forward from the header; records grow backward from the
# end of the page. A slot is (offset:2, length:2); length == 0 means deleted.
PAGE_HEADER_SIZE = 4
SLOT_SIZE = 4
