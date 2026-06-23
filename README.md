# MiniDB — A Working Relational Database Engine

> Advanced DBMS Capstone Project · Extension Track **C — Modern Storage (LSM-tree)**

MiniDB is a from-scratch relational database engine written in pure Python (no
third-party dependencies). It integrates a page-based storage engine, a B+ tree
index, a SQL parser, a cost-based optimizer, a Volcano-style execution engine,
strict-2PL transactions with deadlock detection, and WAL-based crash recovery —
plus an LSM-tree storage engine as the extension track, benchmarked against the
default B+ tree storage.

---

## Team Information

**Team Name:** PageFault

| Full Name | Roll Number | Scaler Email |
|---|---|---|
| Shambhu Yadav | 10356 | shambhu.24bcs10356@scaler.com |
| Sudharsan | 10077 | sudharsan.23bcs10077@sst.scaler.com |
| Krishna Patidar | 10036 | krishna.23bcs10036@sst.scaler.com |
| Arjun Kshirsagar |  |  |

<!-- Fill in the table above before submitting. -->

---

## 1. Project Overview

**Problem statement.** Modern applications rely on databases that must
simultaneously guarantee durability, isolation, efficient lookups, and good
performance under concurrency — all while surviving crashes. Understanding *how*
a database delivers these guarantees requires building one. MiniDB is that
exercise: a small but complete engine where every layer (bytes on disk → SQL
results) is implemented and observable.

**Goals.**
- Implement all required core components and make each individually demonstrable.
- Keep the architecture modular and readable enough to defend in a viva.
- Implement one extension track and quantify its trade-offs with benchmarks.

**Chosen extension track: C — Modern Storage.** We add an LSM-tree storage
engine (MemTable → SSTables → leveled compaction, with per-SSTable Bloom
filters) and benchmark it against the default heap-file + B+ tree storage on
write throughput, read latency, and space/write amplification.

---

## 2. System Architecture

```
                            ┌──────────────────────────┐
            SQL string ───► │  Parser (tokenize + AST)  │   minidb/sql/
                            └─────────────┬─────────────┘
                                          ▼
                            ┌──────────────────────────┐
                            │  Cost-Based Optimizer     │   minidb/optimizer/
                            │  (selectivity, scan/join) │
                            └─────────────┬─────────────┘
                                          ▼  plan (operator tree)
                            ┌──────────────────────────┐
                            │  Executor (Volcano model) │   minidb/execution/
                            │  SeqScan IndexScan Filter │
                            │  NestedLoopJoin           │
                            └─────────────┬─────────────┘
            ┌─────────────────────────────┼─────────────────────────────┐
            ▼                             ▼                             ▼
  ┌──────────────────┐        ┌────────────────────┐        ┌────────────────────┐
  │  Transaction Mgr │        │  Table (access)    │        │   Recovery (WAL)   │
  │  + Lock Manager  │◄──────►│  heap + B+ tree     │───────►│  redo log + crash  │
  │  Strict 2PL,     │ locks  │  index, statistics  │  log   │  recovery          │
  │  deadlock detect │        └─────────┬──────────┘        └────────────────────┘
  └──────────────────┘                  ▼
   minidb/txn/             ┌────────────────────────────┐
                           │  Buffer Pool (LRU, pin,     │   minidb/storage/
                           │  dirty tracking)            │
                           └─────────────┬──────────────┘
                                         ▼
                           ┌────────────────────────────┐
                           │  Disk Manager (page I/O)    │
                           │  + Heap files (slotted pgs) │
                           └────────────────────────────┘

   Extension Track C (alternative storage):  minidb/lsm/
   MemTable ─► immutable MemTable ─► L0 SSTables ─► (compaction) ─► L1 SSTables
   each SSTable carries a Bloom filter + sparse index.
```

**Major modules** (`minidb/`): `storage/` (page, disk_manager, buffer_pool,
heap_file), `index/bplus_tree.py`, `lsm/` (memtable→sstable→engine + bloom),
`catalog/` (schema + metadata), `sql/` (tokenizer, parser, AST),
`optimizer/optimizer.py`, `execution/` (operators, executor), `txn/`
(lock_manager, transaction), `recovery/wal.py`, and `engine.py` (the facade).

**Data flow.** `Database.execute(sql)` parses the statement, asks the optimizer
for a plan, and runs it through the executor. Reads/writes go through the
transaction-aware `Table`, which acquires locks, mutates the heap + B+ tree, and
appends WAL records. Pages live in the buffer pool and reach disk only via
eviction or checkpoint.

---

## 3. Storage Layer

**Page format (slotted page — `storage/page.py`).** Each 4 KB page has a 4-byte
header `(num_slots, free_ptr)` followed by a slot directory growing forward;
records grow backward from the end of the page. A slot is `(offset, length)`;
`length == 0` is a tombstone. This supports variable-length records and stable
record ids (RIDs) of the form `(page_id, slot)`.

**Heap files (`storage/heap_file.py`).** An unordered collection of slotted
pages holding one table's rows. `insert` appends to the last page with room (or
grows the file); `scan` walks every page. The page list per table is owned by
the catalog so it survives restarts.

**Buffer pool (`storage/buffer_pool.py`).** Caches a fixed number of frames
(`BUFFER_POOL_FRAMES = 64`). `fetch_page` serves hits from memory and reads
misses from disk; pages are **pinned** while in use and carry a **dirty** flag.
Replacement is **LRU over unpinned frames**; a dirty victim is written back
before eviction. `stats()` exposes hit ratio and residency — used in demos.

**Disk manager (`storage/disk_manager.py`).** The only layer doing real
syscalls: `page_id * PAGE_SIZE` byte offsets, `allocate_page`, and
`write_page` with `fsync` for durability. Tracks read/write counters.

---

## 4. Indexing

**B+ tree (`index/bplus_tree.py`)** maps the primary key → RID.

- **Node structure.** Internal nodes hold up to `order-1` keys and `order`
  child pointers and only route searches. Leaf nodes hold `(key, value)` pairs
  and a `next` pointer linking leaves left-to-right for range scans. All data
  lives in the leaves.
- **Search path.** From the root, `bisect_right` on the separator keys chooses
  the child to descend into until a leaf is reached, then `bisect_left` locates
  the key. `last_search_path` records the number of nodes visited (the tree
  height), which the demos print.
- **Insert + page splits.** Insertion recurses to the target leaf. On overflow
  a leaf **splits** in half (first right key copied up); internal overflow
  splits with the median **pushed up**; a root split grows the tree by a level.
- **Delete.** Removes the leaf entry and collapses a thinned single-child root.
- **Range scan.** Descends to the lower bound then walks the leaf `next` chain.

The index is rebuilt from the heap on startup, decoupling it from the pager
while still demonstrating real B+ tree mechanics (split/search-path).

---

## 5. Query Execution

**Parser (`sql/parser.py`).** A regex tokenizer feeds a recursive-descent
parser producing AST nodes (`sql/ast.py`) for `CREATE TABLE`, `INSERT`,
`SELECT` (with `WHERE`, `JOIN … ON`), and `DELETE`. Predicates are
`column OP value` (`= != < <= > >=`) combined with `AND`.

**Plan generation.** The optimizer turns the AST into a tree of physical
operators (see §6). `EXPLAIN <select>` renders that tree with cost estimates.

**Operator execution (`execution/operators.py`, Volcano/iterator model).** Each
operator yields dict-rows keyed by `table.col` (and bare `col`):
- `SeqScan` — full heap scan.
- `IndexScan` — primary-key equality point lookup or `[lo, hi]` range via the B+ tree.
- `Filter` — applies residual predicates.
- `NestedLoopJoin` — index nested-loop join when the inner join key is the
  inner table's primary key, otherwise block nested-loop.
The executor (`execution/executor.py`) drives the root operator and projects the
requested columns; DDL/DML statements are applied through the transactional
`Table` API.

---

## 6. Optimizer

`optimizer/optimizer.py` is cost-based:

- **Selectivity estimation.** Equality on the primary key → `1/n_rows`; equality
  on a non-key column → default `0.2`; range predicates → `0.33`.
- **Scan selection.** For each table it compares `SeqScan` cost (`n_rows`)
  against an `IndexScan` cost (`~tree height` for equality, `height + est_rows`
  for a range) and picks the cheaper. *A primary-key equality picks IndexScan; a
  broad non-key filter picks SeqScan* — verified by EXPLAIN in the demos.
- **Join ordering.** For a two-table join it builds both orderings
  (`A outer / B inner` vs `B outer / A inner`), costs each (index NLJ =
  `outer_rows × probe`; block NLJ = `outer_rows × inner_rows`), and keeps the
  cheaper. Multi-way joins chain in declaration order (see Limitations).

Cost model and chosen plan are printed by `EXPLAIN`.

---

## 7. Transactions & Concurrency

`txn/lock_manager.py`, `txn/transaction.py`, lifecycle in `engine.py`.

- **Locking strategy — Strict 2PL.** Shared (read) and Exclusive (write) locks
  at row granularity (`table:key`) plus a table-level shared lock for scans.
  Compatibility: S/S compatible, everything else conflicts. All locks are held
  until commit/abort and released together (strict 2PL ⇒ recoverable, no cascading aborts).
- **Isolation guarantee.** Serializable: writers take X locks, scans take a
  table S lock, so conflicting schedules cannot interleave non-serializably.
- **Deadlock handling.** Before a transaction blocks, its edges are added to a
  **wait-for graph** and DFS checks for a cycle. If waiting would create one,
  the requester is chosen as the **victim** and aborted (`DeadlockError`), and
  its in-memory changes are rolled back via the undo list.

Demonstrated in `demos/demo_concurrency.py` (concurrent shared reads don't
block; an opposite-order X-lock pattern triggers detection and one abort).

---

## 8. Recovery

`recovery/wal.py`.

- **WAL design.** Newline-delimited JSON records (inspectable in the demo):
  `BEGIN`, `INSERT`, `UPDATE` (before+after images), `DELETE` (before image),
  `COMMIT`, `ABORT`, `CHECKPOINT`. **Durability rule:** the log is `fsync`'d
  before a `COMMIT` is acknowledged (WAL invariant).
- **Buffer policy.** NO-FORCE (commit does not flush data pages) + NO-STEAL
  (dirty pages reach disk only at a checkpoint), so uncommitted changes never
  reach the data file.
- **Crash recovery (pure redo).** On startup the engine scans the log after the
  last checkpoint, identifies winners (transactions with a `COMMIT`), and
  **redoes** their operations in log order. Losers are ignored — their effects
  never reached disk, so no undo pass is needed. `checkpoint()` flushes pages,
  persists the catalog, and truncates the log.

Demonstrated in `demos/demo_crash_recovery.py` (committed rows survive a
simulated crash; an uncommitted transaction's row is gone).

---

## 9. Extension Track C — LSM-Tree Storage

`minidb/lsm/` (`memtable` semantics in `lsm_engine.py`, `sstable.py`,
`bloom.py`).

- **Motivation.** B+ tree / heap storage does in-place, random writes. An
  LSM-tree converts random updates into **sequential appends**, trading write
  amplification later (compaction) for much higher write throughput now — the
  right shape for write-heavy workloads.
- **Design.**
  - *Write path:* `put`/`delete` go to an in-memory **MemTable** (a dict; delete
    writes a tombstone). When it exceeds `memtable_limit` it becomes immutable
    and is flushed to a new **L0 SSTable** (immutable, sorted, with a sparse
    index + Bloom filter).
  - *Read path:* check MemTable → immutable MemTables → L0 (newest first) → L1.
    First hit wins (newest version); a tombstone means deleted. Bloom filters
    skip SSTables that cannot contain the key with **zero disk reads**.
  - *Compaction:* when L0 accumulates `L0_COMPACTION_TRIGGER` tables they are
    merged with L1 into one sorted, non-overlapping run, dropping shadowed
    versions and tombstones.
- **Results.** See §10 — LSM achieves ~4× write throughput at the cost of higher
  read latency and write/space amplification.

---

## 10. Benchmarks

**Setup.** `benchmarks/bench_lsm_vs_btree.py`, N = 50,000 keys (~50-byte rows,
~2.5 MB logical), random point lookups for hits and 5,000 absent keys for
misses. Same workload on both engines. (`python3 -m benchmarks.bench_lsm_vs_btree`.)

| Metric | B+Tree (heap) | LSM-tree |
|---|---|---|
| Write throughput (ops/s) | ~46,000 | ~197,000 |
| Point read — hit (µs) | ~5.1 | ~22.2 |
| Point read — miss (µs) | ~0.24 | ~2.9 |
| Space amplification | 1.09× | 1.33× |
| Write amplification | 1.00× | 2.93× |
| Bloom-filter skips (5k misses) | 0 | 23,219 |

**Analysis.**
- **Write throughput (LSM ~4.3× faster).** LSM appends to an in-memory MemTable
  and flushes sequentially; the B+ tree path pays for an index descent + heap
  insert per row.
- **Read latency (LSM ~4× slower on hits).** A read may probe the MemTable plus
  several SSTables across levels — this is **read amplification**, the price of
  cheap writes. Bloom filters keep it bounded: 23,219 SSTable reads were skipped
  across 5,000 negative lookups.
- **Space & write amplification.** LSM keeps superseded versions until
  compaction (1.33× space) and rewrites data during compaction (2.93× bytes
  written), versus the in-place B+ tree (≈1×). This is the **amplification
  triangle**: you cannot minimize write, read, and space amplification at once.

---

## 11. Limitations

- **B+ tree deletes** use lazy leaf deletion + root collapse rather than full
  borrow/merge rebalancing; the tree stays correct and ordered but can become
  less compact under heavy deletion.
- **Indexes are in-memory**, rebuilt from the heap on startup (not paged to
  disk); fine for the demonstrated scales, not for datasets exceeding RAM.
- **Recovery assumes NO-STEAL**, enforced by flushing only at checkpoints; the
  buffer pool must hold the working set between checkpoints.
- **Optimizer** handles single-table scans and (cost-ordered) two-table joins;
  3+ way joins chain in declaration order. No aggregation/GROUP BY or ORDER BY.
- **SQL surface** is intentionally small (no `UPDATE` statement — updates are
  exposed via the API; no subqueries, no `OR` in `WHERE`).
- **Single-process**; the LSM engine and the transactional engine are separate
  storage paths (the extension is benchmarked standalone, not wired under SQL).

Future improvements: paged/persistent B+ tree, MVCC, ORDER BY/aggregation,
WAL-backed LSM, and wiring LSM as a per-table storage option under SQL.

---

## 12. How to Run

**Dependencies.** Python 3.8+ only — **no third-party packages** (stdlib only).

```bash
cd MiniDB

# 1) Run the test suite (covers every component)
python3 tests/test_minidb.py
#   or:  pytest -q

# 2) Demos
python3 demos/demo_sql.py             # SQL + EXPLAIN (IndexScan/SeqScan/join)
python3 demos/demo_crash_recovery.py  # WAL crash recovery
python3 demos/demo_concurrency.py     # 2PL + deadlock detection

# 3) Benchmark (Extension Track C)
python3 -m benchmarks.bench_lsm_vs_btree 50000

# 4) Interactive shell
python3 -m minidb.cli mydata
```

**Example session:**
```sql
minidb> CREATE TABLE users (id INT PRIMARY KEY, name TEXT, city_id INT);
minidb> INSERT INTO users VALUES (1, 'Asha', 2);
minidb> EXPLAIN SELECT id, name FROM users WHERE id = 1;
minidb> SELECT id, name FROM users WHERE id = 1;
minidb> .stats
minidb> .checkpoint
minidb> .exit
```
