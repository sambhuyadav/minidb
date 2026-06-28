# MiniDB - Architecture Notes

This is a condensed map of the codebase; see the root `README.md` for the full
design discussion, trade-offs, and benchmarks.

## Layer / file map

| Layer | Files | Responsibility |
|---|---|---|
| Disk I/O | `minidb/storage/disk_manager.py` | page-grained read/write/allocate, `fsync` |
| Caching | `minidb/storage/buffer_pool.py` | LRU buffer pool, pin/dirty, eviction |
| Pages | `minidb/storage/page.py` | slotted page layout (variable-length records) |
| Tables | `minidb/engine.py`, `minidb/storage/heap_file.py` | LSM-backed SQL tables plus heap baseline |
| Index | `minidb/index/bplus_tree.py` | B+ tree: search / insert+split / delete / range |
| Catalog | `minidb/catalog/` | schema, row (de)serialization, metadata persistence |
| Parser | `minidb/sql/` | tokenizer, recursive-descent parser, AST |
| Optimizer | `minidb/optimizer/optimizer.py` | selectivity, scan choice, join ordering, EXPLAIN |
| Execution | `minidb/execution/` | Volcano operators + executor |
| Transactions | `minidb/txn/` | Strict 2PL lock manager, deadlock detection, txn |
| Recovery | `minidb/recovery/wal.py` | WAL records, redo/undo crash recovery |
| Facade | `minidb/engine.py` | `Database` + transactional `Table`, `execute()` |
| **Extension C** | `minidb/lsm/` | MemTable to SSTables to compaction, Bloom filters |

## Request lifecycle (a SELECT)

1. `Database.execute(sql)` calls `sql.parser.parse` and produces an AST.
2. `optimizer.Optimizer.plan(ast)` produces an operator tree with cost estimates.
3. `execution.Executor.run(plan)` drives the root operator (Volcano `rows()`),
   acquiring S-locks via the lock manager as it touches data.
4. Rows are projected to the requested columns and returned.

## Write lifecycle (an INSERT, inside a transaction)

1. The table access method acquires a table X-lock (phantom prevention) and row
   X-lock.
2. A WAL `INSERT` record is appended and forced before row bytes can dirty heap
   pages or mutate the LSM MemTable.
3. In the default LSM path, `LSMTable.insert` writes the serialized row to the
   LSM MemTable and indexes the primary key in the B+ tree.
4. In the heap baseline, `Table.insert` writes the serialized row to a heap
   page and indexes the primary key to its RID in the B+ tree.
5. On `COMMIT`: WAL `COMMIT` is forced and locks release. On `ABORT`:
   in-memory undo rolls back the active changes.
6. On `checkpoint()`: active transactions are rejected, LSM MemTables flush to
   SSTables, heap baseline pages flush, catalog metadata persists, then WAL is
   truncated.

## Durability / recovery model

NO-FORCE means commit does not force data pages. On startup the engine replays
committed transactions after the last checkpoint and undoes loser operations in
reverse log order if they reached disk through heap buffer eviction. Table
storage kind is persisted in the catalog so LSM-created and heap-created tables
reopen with the correct access method.
