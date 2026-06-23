# MiniDB — Architecture Notes

This is a condensed map of the codebase; see the root `README.md` for the full
design discussion, trade-offs, and benchmarks.

## Layer / file map

| Layer | Files | Responsibility |
|---|---|---|
| Disk I/O | `minidb/storage/disk_manager.py` | page-grained read/write/allocate, `fsync` |
| Caching | `minidb/storage/buffer_pool.py` | LRU buffer pool, pin/dirty, eviction |
| Pages | `minidb/storage/page.py` | slotted page layout (variable-length records) |
| Tables | `minidb/storage/heap_file.py` | heap file (unordered slotted pages) |
| Index | `minidb/index/bplus_tree.py` | B+ tree: search / insert+split / delete / range |
| Catalog | `minidb/catalog/` | schema, row (de)serialization, metadata persistence |
| Parser | `minidb/sql/` | tokenizer, recursive-descent parser, AST |
| Optimizer | `minidb/optimizer/optimizer.py` | selectivity, scan choice, join ordering, EXPLAIN |
| Execution | `minidb/execution/` | Volcano operators + executor |
| Transactions | `minidb/txn/` | Strict 2PL lock manager, deadlock detection, txn |
| Recovery | `minidb/recovery/wal.py` | WAL records, redo-based crash recovery |
| Facade | `minidb/engine.py` | `Database` + transactional `Table`, `execute()` |
| **Extension C** | `minidb/lsm/` | MemTable → SSTables → compaction, Bloom filters |

## Request lifecycle (a SELECT)

1. `Database.execute(sql)` → `sql.parser.parse` → AST.
2. `optimizer.Optimizer.plan(ast)` → operator tree (with cost estimates).
3. `execution.Executor.run(plan)` drives the root operator (Volcano `rows()`),
   acquiring S-locks via the lock manager as it touches data.
4. Rows are projected to the requested columns and returned.

## Write lifecycle (an INSERT, inside a transaction)

1. `Table.insert` acquires an X-lock on `table:key`.
2. Row is serialized, appended to the heap, indexed in the B+ tree.
3. A WAL `INSERT` record is appended (forced to disk on `COMMIT`).
4. On `COMMIT`: WAL `fsync` + release all locks. On `ABORT`: in-memory undo.

## Durability / recovery model

NO-FORCE + NO-STEAL → pure-redo recovery. Data pages reach disk only at
`checkpoint()`; committed changes between checkpoints are reconstructable from
the WAL. On startup the engine replays committed transactions after the last
checkpoint and ignores losers.
