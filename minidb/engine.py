"""The MiniDB engine facade.

`Table` is the transaction-aware access method over a heap file + B+ tree
primary-key index. `Database` wires together the disk manager, buffer pool,
catalog, WAL, lock manager and transaction lifecycle, and exposes `execute()`
to run SQL end-to-end (parse -> optimize -> execute).
"""

import os

from .constants import BUFFER_POOL_FRAMES
from .storage.disk_manager import DiskManager
from .storage.buffer_pool import BufferPool
from .storage.heap_file import HeapFile
from .index.bplus_tree import BPlusTree
from .catalog.catalog import Catalog
from .catalog.schema import Schema
from .txn.lock_manager import LockManager, SHARED, EXCLUSIVE
from .txn.transaction import Transaction, COMMITTED, ABORTED
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
        self.pk = schema.pk_index
        self.heap = HeapFile(db.bp, db.catalog.page_ids(self.name))
        self.index = BPlusTree()           # primary key -> RID
        self.stats = Statistics()

    # --- lock helpers -------------------------------------------------------
    def _row_res(self, key):
        return f"{self.name}:{key}"

    def _table_res(self):
        return f"{self.name}:*"

    # --- transactional writes ----------------------------------------------
    def insert(self, txn: Transaction, row):
        key = row[self.pk]
        self.db.locks.acquire(txn.txn_id, self._row_res(key), EXCLUSIVE)
        txn.locks.add(self._row_res(key))
        if self.index.search(key) is not None:
            raise ValueError(f"duplicate primary key {key!r} in {self.name}")
        rid = self.heap.insert(self.schema.serialize(tuple(row)))
        self.index.insert(key, rid)
        self.stats.observe_insert(key)
        self.db.wal.log_insert(txn.txn_id, self.name, key, row)
        txn.undo.append(lambda: self._undo_insert(key, rid))
        return rid

    def delete_by_key(self, txn: Transaction, key):
        self.db.locks.acquire(txn.txn_id, self._row_res(key), EXCLUSIVE)
        txn.locks.add(self._row_res(key))
        rid = self.index.search(key)
        if rid is None:
            return False
        old = self.schema.deserialize(self.heap.get(rid))
        self.heap.delete(rid)
        self.index.delete(key)
        self.stats.observe_delete()
        self.db.wal.log_delete(txn.txn_id, self.name, key, old)
        txn.undo.append(lambda: self._undo_delete(key, old))
        return True

    def update_by_key(self, txn: Transaction, key, new_row):
        self.db.locks.acquire(txn.txn_id, self._row_res(key), EXCLUSIVE)
        txn.locks.add(self._row_res(key))
        rid = self.index.search(key)
        if rid is None:
            return False
        old = self.schema.deserialize(self.heap.get(rid))
        new_rid = self.heap.update(rid, self.schema.serialize(tuple(new_row)))
        if new_rid != rid:
            self.index.insert(key, new_rid)
        self.db.wal.log_update(txn.txn_id, self.name, key, old, new_row)
        txn.undo.append(lambda: self._undo_update(key, old, rid))
        return True

    # --- transactional reads ------------------------------------------------
    def get_by_key(self, txn: Transaction, key):
        self.db.locks.acquire(txn.txn_id, self._row_res(key), SHARED)
        txn.locks.add(self._row_res(key))
        rid = self.index.search(key)
        if rid is None:
            return None
        return self.schema.deserialize(self.heap.get(rid))

    def seq_scan(self, txn: Transaction):
        self.db.locks.acquire(txn.txn_id, self._table_res(), SHARED)
        txn.locks.add(self._table_res())
        for rid, rec in self.heap.scan():
            yield self.schema.deserialize(rec)

    def index_range(self, txn: Transaction, lo=None, hi=None):
        self.db.locks.acquire(txn.txn_id, self._table_res(), SHARED)
        txn.locks.add(self._table_res())
        for key, rid in self.index.range_scan(lo, hi):
            yield self.schema.deserialize(self.heap.get(rid))

    # --- in-memory undo (abort) --------------------------------------------
    def _undo_insert(self, key, rid):
        self.heap.delete(rid)
        self.index.delete(key)
        self.stats.observe_delete()

    def _undo_delete(self, key, old):
        rid = self.heap.insert(self.schema.serialize(tuple(old)))
        self.index.insert(key, rid)
        self.stats.observe_insert(key)

    def _undo_update(self, key, old, rid):
        new_rid = self.heap.update(rid, self.schema.serialize(tuple(old)))
        if new_rid != rid:
            self.index.insert(key, new_rid)

    # --- redo (recovery) — no locks, no logging ----------------------------
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


class Database:
    def __init__(self, directory: str):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)
        self.disk = DiskManager(os.path.join(directory, "minidb.data"))
        self.bp = BufferPool(self.disk, capacity=BUFFER_POOL_FRAMES)
        self.catalog = Catalog(os.path.join(directory, "catalog.json"))
        self.wal = WriteAheadLog(os.path.join(directory, "wal.log"))
        self.locks = LockManager()
        self.tables = {}
        self._load_tables()
        self.recover_on_start()

    # --- table management ---------------------------------------------------
    def _load_tables(self):
        for name in self.catalog.tables:
            schema = self.catalog.schema(name)
            table = Table(self, schema)
            # rebuild the in-memory B+ tree index from the heap
            for rid, rec in table.heap.scan():
                row = schema.deserialize(rec)
                table.index.insert(row[schema.pk_index], rid)
                table.stats.observe_insert(row[schema.pk_index])
            self.tables[name] = table

    def create_table(self, schema: Schema) -> Table:
        self.catalog.add_table(schema)
        table = Table(self, schema)
        self.tables[schema.table] = table
        return table

    def get_table(self, name) -> Table:
        if name not in self.tables:
            raise KeyError(f"no such table: {name}")
        return self.tables[name]

    # --- transactions -------------------------------------------------------
    def begin(self) -> Transaction:
        txn = Transaction()
        self.wal.log_begin(txn.txn_id)
        return txn

    def commit(self, txn: Transaction):
        self.wal.log_commit(txn.txn_id)     # forces WAL fsync (durability)
        self.locks.release_all(txn.txn_id)
        txn.state = COMMITTED

    def abort(self, txn: Transaction):
        for undo in reversed(txn.undo):     # roll back in reverse order
            undo()
        self.wal.log_abort(txn.txn_id)
        self.locks.release_all(txn.txn_id)
        txn.state = ABORTED

    # --- checkpoint + recovery ---------------------------------------------
    def checkpoint(self):
        """Flush all dirty data pages, persist catalog, then truncate the WAL."""
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
        self.last_recovery = recover(self.wal, apply_fn)
        return self.last_recovery

    # --- SQL ----------------------------------------------------------------
    def execute(self, sql: str, txn: Transaction = None):
        """Parse, optimize and execute one SQL statement.

        If no transaction is supplied, runs in auto-commit mode.
        """
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
            if own_txn:
                self.abort(txn)
            raise

    def explain(self, sql: str) -> str:
        """Return the EXPLAIN plan text for a SELECT, with estimates."""
        from .sql.parser import parse
        from .optimizer.optimizer import Optimizer, explain
        stmt = parse(sql)
        plan = Optimizer(self).plan(stmt)
        if plan is None:
            return "(no plan: not a SELECT)"
        return explain(plan)

    def close(self):
        self.bp.flush_all()
        self.catalog.save()
        self.disk.close()
        self.wal.close()
