"""The MiniDB engine facade.

`Table` is the transaction-aware heap baseline over a heap file + B+ tree
primary-key index. `LSMTable` is the Track C SQL access method over an LSM row
store plus the same primary-key index abstraction. `Database` wires together
the disk manager, buffer pool, catalog, WAL, lock manager and transaction
lifecycle, reopens each table by its persisted storage kind, and exposes
`execute()` to run SQL end-to-end (parse -> optimize -> execute).
"""

import os

from .constants import BUFFER_POOL_FRAMES
from .storage.disk_manager import DiskManager
from .storage.buffer_pool import BufferPool
from .storage.heap_file import HeapFile
from .index.bplus_tree import BPlusTree
from .lsm.lsm_engine import LSMEngine
from .catalog.catalog import Catalog
from .catalog.schema import Schema
from .txn.lock_manager import LockManager, SHARED, EXCLUSIVE
from .txn.transaction import Transaction, COMMITTED, ABORTED, IN_DOUBT
from .recovery.wal import WriteAheadLog, recover


class Statistics:
    """Per-table statistics the cost-based optimizer consumes."""
    def __init__(self):
        self.n_rows = 0
        self.min_key = None
        self.max_key = None
        self.distinct = {}     # column index -> set of sampled values (small tables)

    def observe_insert(self, key):
        self.n_rows += 1
        if self.min_key is None or key < self.min_key:
            self.min_key = key
        if self.max_key is None or key > self.max_key:
            self.max_key = key

    def observe_delete(self):
        self.n_rows = max(0, self.n_rows - 1)


class Table:
    def __init__(self, db: "Database", schema: Schema):
        self.db = db
        self.schema = schema
        self.name = schema.table
        self.storage_kind = "heap"
        self.pk = schema.pk_index
        self.heap = HeapFile(db.bp, db.catalog.page_ids(self.name))
        self.index = BPlusTree()           # primary key -> RID
        self.stats = Statistics()
        for rid, rec in self.heap.scan():
            row = self.schema.deserialize(rec)
            key = row[self.pk]
            self.index.insert(key, rid)
            self.stats.observe_insert(key)

    # --- lock helpers -------------------------------------------------------
    def _row_res(self, key):
        return f"{self.name}:{key}"

    def _table_res(self):
        return f"{self.name}:*"

    # --- transactional writes ----------------------------------------------
    def insert(self, txn: Transaction, row):
        self.db._ensure_open()
        key = row[self.pk]
        # We use a table X lock for inserts so phantom prevention is explicit
        # in the strict-2PL design, even though it limits concurrency.
        self.db.locks.acquire(txn.txn_id, self._table_res(), EXCLUSIVE)
        txn.locks.add(self._table_res())
        self.db.locks.acquire(txn.txn_id, self._row_res(key), EXCLUSIVE)
        txn.locks.add(self._row_res(key))
        if self.index.search(key) is not None:
            raise ValueError(f"duplicate primary key {key!r} in {self.name}")
        self.db.wal.log_insert(txn.txn_id, self.name, key, row)
        self.db.wal.flush()
        rid = self.heap.insert(self.schema.serialize(tuple(row)))
        self.index.insert(key, rid)
        self.stats.observe_insert(key)
        txn.undo.append(lambda: self._undo_insert(key, rid))
        return rid

    def delete_by_key(self, txn: Transaction, key):
        self.db._ensure_open()
        self.db.locks.acquire(txn.txn_id, self._row_res(key), EXCLUSIVE)
        txn.locks.add(self._row_res(key))
        rid = self.index.search(key)
        if rid is None:
            return False
        old = self.schema.deserialize(self.heap.get(rid))
        self.db.wal.log_delete(txn.txn_id, self.name, key, old)
        self.db.wal.flush()
        self.heap.delete(rid)
        self.index.delete(key)
        self.stats.observe_delete()
        txn.undo.append(lambda: self._undo_delete(key, old))
        return True

    def update_by_key(self, txn: Transaction, key, new_row):
        self.db._ensure_open()
        self.db.locks.acquire(txn.txn_id, self._row_res(key), EXCLUSIVE)
        txn.locks.add(self._row_res(key))
        rid = self.index.search(key)
        if rid is None:
            return False
        old = self.schema.deserialize(self.heap.get(rid))
        self.db.wal.log_update(txn.txn_id, self.name, key, old, new_row)
        self.db.wal.flush()
        new_rid = self.heap.update(rid, self.schema.serialize(tuple(new_row)))
        if new_rid != rid:
            self.index.insert(key, new_rid)
        txn.undo.append(lambda: self._undo_update(key, old, rid))
        return True

    # --- transactional reads ------------------------------------------------
    def get_by_key(self, txn: Transaction, key):
        self.db._ensure_open()
        self.db.locks.acquire(txn.txn_id, self._row_res(key), SHARED)
        txn.locks.add(self._row_res(key))
        rid = self.index.search(key)
        if rid is None:
            return None
        return self.schema.deserialize(self.heap.get(rid))

    def seq_scan(self, txn: Transaction):
        self.db._ensure_open()
        self.db.locks.acquire(txn.txn_id, self._table_res(), SHARED)
        txn.locks.add(self._table_res())
        for rid, rec in self.heap.scan():
            row = self.schema.deserialize(rec)
            key = row[self.pk]
            self.db.locks.acquire(txn.txn_id, self._row_res(key), SHARED)
            txn.locks.add(self._row_res(key))
            yield row

    def index_range(self, txn: Transaction, lo=None, hi=None):
        self.db._ensure_open()
        self.db.locks.acquire(txn.txn_id, self._table_res(), SHARED)
        txn.locks.add(self._table_res())
        for key, rid in self.index.range_scan(lo, hi):
            self.db.locks.acquire(txn.txn_id, self._row_res(key), SHARED)
            txn.locks.add(self._row_res(key))
            yield self.schema.deserialize(self.heap.get(rid))

    # --- in-memory undo (abort) --------------------------------------------
    def _undo_insert(self, key, rid):
        rid = self.index.search(key)
        if rid is not None:
            self.heap.delete(rid)
            self.index.delete(key)
            self.stats.observe_delete()

    def _undo_delete(self, key, old):
        self._restore_key(key, old)

    def _undo_update(self, key, old, rid):
        self._restore_key(key, old)

    def _restore_key(self, key, row):
        serial = self.schema.serialize(tuple(row))
        rid = self.index.search(key)
        if rid is None:
            new_rid = self.heap.insert(serial)
            self.index.insert(key, new_rid)
            self.stats.observe_insert(key)
            return
        new_rid = self.heap.update(rid, serial)
        if new_rid != rid:
            self.index.insert(key, new_rid)

    # --- redo (recovery) - no locks, no logging ----------------------------
    def redo_insert(self, key, row):
        if self.index.search(key) is not None:
            return
        rid = self.heap.insert(self.schema.serialize(tuple(row)))
        self.index.insert(key, rid)
        self.stats.observe_insert(key)

    def redo_update(self, key, row):
        rid = self.index.search(key)
        if rid is None:
            return self.redo_insert(key, row)
        new_rid = self.heap.update(rid, self.schema.serialize(tuple(row)))
        if new_rid != rid:
            self.index.insert(key, new_rid)

    def redo_delete(self, key):
        rid = self.index.search(key)
        if rid is None:
            return
        self.heap.delete(rid)
        self.index.delete(key)
        self.stats.observe_delete()


class LSMTable:
    """Transaction-aware table access over an LSM-tree plus B+ tree PK index."""
    def __init__(self, db: "Database", schema: Schema):
        self.db = db
        self.schema = schema
        self.name = schema.table
        self.storage_kind = "lsm"
        self.pk = schema.pk_index
        self.lsm = LSMEngine(os.path.join(db.dir, "lsm", self.name),
                             memtable_limit=db.lsm_memtable_limit,
                             auto_flush=False)
        self.index = BPlusTree()
        self.stats = Statistics()
        for _, rec in self.lsm.scan():
            row = self.schema.deserialize(rec)
            key = row[self.pk]
            self.index.insert(key, key)
            self.stats.observe_insert(key)

    # --- lock helpers -------------------------------------------------------
    def _row_res(self, key):
        return f"{self.name}:{key}"

    def _table_res(self):
        return f"{self.name}:*"

    def _get_unlocked(self, key):
        rec = self.lsm.get(key)
        return self.schema.deserialize(rec) if rec is not None else None

    # --- transactional writes ----------------------------------------------
    def insert(self, txn: Transaction, row):
        self.db._ensure_open()
        key = row[self.pk]
        self.db.locks.acquire(txn.txn_id, self._table_res(), EXCLUSIVE)
        txn.locks.add(self._table_res())
        self.db.locks.acquire(txn.txn_id, self._row_res(key), EXCLUSIVE)
        txn.locks.add(self._row_res(key))
        if self.index.search(key) is not None:
            raise ValueError(f"duplicate primary key {key!r} in {self.name}")
        self.db.wal.log_insert(txn.txn_id, self.name, key, row)
        self.db.wal.flush()
        self.lsm.put(key, self.schema.serialize(tuple(row)))
        self.index.insert(key, key)
        self.stats.observe_insert(key)
        txn.undo.append(lambda: self._undo_insert(key))
        return key

    def delete_by_key(self, txn: Transaction, key):
        self.db._ensure_open()
        self.db.locks.acquire(txn.txn_id, self._row_res(key), EXCLUSIVE)
        txn.locks.add(self._row_res(key))
        old = self._get_unlocked(key)
        if old is None:
            return False
        self.db.wal.log_delete(txn.txn_id, self.name, key, old)
        self.db.wal.flush()
        self.lsm.delete(key)
        self.index.delete(key)
        self.stats.observe_delete()
        txn.undo.append(lambda: self._undo_delete(key, old))
        return True

    def update_by_key(self, txn: Transaction, key, new_row):
        self.db._ensure_open()
        self.db.locks.acquire(txn.txn_id, self._row_res(key), EXCLUSIVE)
        txn.locks.add(self._row_res(key))
        old = self._get_unlocked(key)
        if old is None:
            return False
        self.db.wal.log_update(txn.txn_id, self.name, key, old, new_row)
        self.db.wal.flush()
        self.lsm.put(key, self.schema.serialize(tuple(new_row)))
        txn.undo.append(lambda: self._undo_update(key, old))
        return True

    # --- transactional reads ------------------------------------------------
    def get_by_key(self, txn: Transaction, key):
        self.db._ensure_open()
        self.db.locks.acquire(txn.txn_id, self._row_res(key), SHARED)
        txn.locks.add(self._row_res(key))
        return self._get_unlocked(key)

    def seq_scan(self, txn: Transaction):
        self.db._ensure_open()
        self.db.locks.acquire(txn.txn_id, self._table_res(), SHARED)
        txn.locks.add(self._table_res())
        for _, rec in self.lsm.scan():
            row = self.schema.deserialize(rec)
            key = row[self.pk]
            self.db.locks.acquire(txn.txn_id, self._row_res(key), SHARED)
            txn.locks.add(self._row_res(key))
            yield row

    def index_range(self, txn: Transaction, lo=None, hi=None):
        self.db._ensure_open()
        self.db.locks.acquire(txn.txn_id, self._table_res(), SHARED)
        txn.locks.add(self._table_res())
        for key, _ in self.index.range_scan(lo, hi):
            self.db.locks.acquire(txn.txn_id, self._row_res(key), SHARED)
            txn.locks.add(self._row_res(key))
            row = self._get_unlocked(key)
            if row is not None:
                yield row

    # --- in-memory undo (abort) --------------------------------------------
    def _undo_insert(self, key):
        self.lsm.delete(key)
        self.index.delete(key)
        self.stats.observe_delete()

    def _undo_delete(self, key, old):
        self.lsm.put(key, self.schema.serialize(tuple(old)))
        self.index.insert(key, key)
        self.stats.observe_insert(key)

    def _undo_update(self, key, old):
        self.lsm.put(key, self.schema.serialize(tuple(old)))

    # --- redo (recovery) - no locks, no logging ----------------------------
    def redo_insert(self, key, row):
        if self.index.search(key) is not None:
            return
        self.lsm.put(key, self.schema.serialize(tuple(row)))
        self.index.insert(key, key)
        self.stats.observe_insert(key)

    def redo_update(self, key, row):
        if self.index.search(key) is None:
            return self.redo_insert(key, row)
        self.lsm.put(key, self.schema.serialize(tuple(row)))

    def redo_delete(self, key):
        if self.index.search(key) is None:
            return
        self.lsm.delete(key)
        self.index.delete(key)
        self.stats.observe_delete()

    def flush(self):
        self.lsm.flush()


class Database:
    def __init__(self, directory: str, storage_kind: str = "lsm",
                 lsm_memtable_limit: int = 1000):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)
        self.storage_kind = storage_kind
        self.lsm_memtable_limit = lsm_memtable_limit
        self._crashed = False
        self.disk = DiskManager(os.path.join(directory, "minidb.data"))
        self.bp = BufferPool(self.disk, capacity=BUFFER_POOL_FRAMES)
        self.catalog = Catalog(os.path.join(directory, "catalog.json"))
        self.wal = WriteAheadLog(os.path.join(directory, "wal.log"))
        self.locks = LockManager()
        self.tables = {}
        self._active_txns = {}
        self._load_tables()
        self.recover_on_start()

    # --- table management ---------------------------------------------------
    def _ensure_open(self):
        if self._crashed:
            raise RuntimeError("database instance has crashed; reopen it before use")

    def _new_table(self, schema: Schema, storage_kind: str):
        if storage_kind == "lsm":
            return LSMTable(self, schema)
        if storage_kind == "heap":
            return Table(self, schema)
        raise ValueError(f"unknown storage kind: {storage_kind}")

    def _load_tables(self):
        for name in self.catalog.tables:
            schema = self.catalog.schema(name)
            self.tables[name] = self._new_table(schema, self.catalog.storage_kind(name))

    def create_table(self, schema: Schema) -> Table:
        self._ensure_open()
        self.catalog.add_table(schema, self.storage_kind)
        table = self._new_table(schema, self.storage_kind)
        self.tables[schema.table] = table
        return table

    def get_table(self, name) -> Table:
        self._ensure_open()
        if name not in self.tables:
            raise KeyError(f"no such table: {name}")
        return self.tables[name]

    # --- transactions -------------------------------------------------------
    def begin(self) -> Transaction:
        self._ensure_open()
        txn = Transaction()
        self.wal.log_begin(txn.txn_id)
        self._active_txns[txn.txn_id] = txn
        return txn

    def commit(self, txn: Transaction):
        self._ensure_open()
        try:
            self.wal.log_commit(txn.txn_id)     # forces WAL fsync (durability)
        except Exception:
            txn.state = IN_DOUBT
            raise
        self.locks.release_all(txn.txn_id)
        self._active_txns.pop(txn.txn_id, None)
        txn.state = COMMITTED

    def abort(self, txn: Transaction):
        self._ensure_open()
        for undo in reversed(txn.undo):     # roll back in reverse order
            undo()
        self.wal.log_abort(txn.txn_id)
        self.locks.release_all(txn.txn_id)
        self._active_txns.pop(txn.txn_id, None)
        txn.state = ABORTED

    # --- checkpoint + recovery ---------------------------------------------
    def checkpoint(self):
        """Flush all dirty data pages, persist catalog, then truncate the WAL."""
        self._ensure_open()
        if self._active_txns:
            raise RuntimeError("checkpoint rejected: active transaction(s) exist")
        for table in self.tables.values():
            flush = getattr(table, "flush", None)
            if flush is not None:
                flush()
        self.bp.flush_all()
        self.catalog.save()
        self.wal.log_checkpoint()
        self.wal.truncate()

    def recover_on_start(self):
        def apply_fn(rec):
            table = self.tables.get(rec["table"])
            if table is None:
                return
            if rec["type"] == "INSERT":
                table.redo_insert(rec["key"], rec["row"])
            elif rec["type"] == "UPDATE":
                table.redo_update(rec["key"], rec["row"])
            elif rec["type"] == "DELETE":
                table.redo_delete(rec["key"])

        def undo_fn(rec):
            table = self.tables.get(rec["table"])
            if table is None:
                return
            if rec["type"] == "INSERT":
                table.redo_delete(rec["key"])
            elif rec["type"] == "UPDATE":
                table.redo_update(rec["key"], rec["old"])
            elif rec["type"] == "DELETE":
                table.redo_insert(rec["key"], rec["old"])

        self.last_recovery = recover(self.wal, apply_fn, undo_fn)
        return self.last_recovery

    # --- SQL ----------------------------------------------------------------
    def execute(self, sql: str, txn: Transaction = None):
        """Parse, optimize and execute one SQL statement.

        If no transaction is supplied, runs in auto-commit mode.
        """
        self._ensure_open()
        from .sql.parser import parse
        from .optimizer.optimizer import Optimizer
        from .execution.executor import Executor

        stmt = parse(sql)
        own_txn = txn is None
        if own_txn:
            txn = self.begin()
        try:
            plan = Optimizer(self).plan(stmt)
            result = Executor(self, txn).run(plan, stmt)
            if own_txn:
                self.commit(txn)
            return result
        except Exception:
            if own_txn and txn.state != IN_DOUBT:
                self.abort(txn)
            raise

    def explain(self, sql: str) -> str:
        """Return the EXPLAIN plan text for a SELECT, with estimates."""
        self._ensure_open()
        from .sql.parser import parse
        from .optimizer.optimizer import Optimizer, explain
        stmt = parse(sql)
        plan = Optimizer(self).plan(stmt)
        if plan is None:
            return "(no plan: not a SELECT)"
        return explain(plan)

    def close(self):
        if self._crashed:
            return
        if self._active_txns:
            raise RuntimeError("close rejected: active transaction(s) exist")
        for table in self.tables.values():
            flush = getattr(table, "flush", None)
            if flush is not None:
                flush()
        self.bp.flush_all()
        self.catalog.save()
        self.disk.close()
        self.wal.close()

    def crash(self):
        """Simulate process death: release OS handles without flushing dirty pages."""
        self._crashed = True
        self.bp._frames.clear()
        self.wal.crash_close()
        self.disk.close()
