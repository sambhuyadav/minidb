"""Test suite for MiniDB. Runnable with `uv run python tests/test_minidb.py`.

Covers every required component: storage, B+ tree, SQL execution, optimizer,
transactions/locking, recovery, and the LSM extension.
"""

import os
import random
import shutil
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minidb.storage.disk_manager import DiskManager
from minidb.storage.buffer_pool import BufferPool
from minidb.storage.heap_file import HeapFile
from minidb.index.bplus_tree import BPlusTree
from minidb.lsm.lsm_engine import LSMEngine
from minidb.engine import Database
from minidb.constants import BUFFER_POOL_FRAMES
from minidb.recovery.wal import WriteAheadLog
from minidb.txn.lock_manager import DeadlockError


TEMP_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         ".minidb_tmp")


def temp_db_dir():
    os.makedirs(TEMP_ROOT, exist_ok=True)
    return tempfile.mkdtemp(dir=TEMP_ROOT)


def force_buffer_eviction(db):
    for _ in range(BUFFER_POOL_FRAMES + 5):
        frame = db.bp.new_page()
        db.bp.unpin_page(frame.page_id, is_dirty=True)


def test_storage_heap_and_buffer():
    d = temp_db_dir()
    disk = DiskManager(os.path.join(d, "t.data"))
    bp = BufferPool(disk, capacity=4)
    hf = HeapFile(bp, [])
    rids = [hf.insert(f"r{i}".encode()) for i in range(300)]
    bp.flush_all()
    assert hf.get(rids[0]) == b"r0"
    hf.delete(rids[5])
    assert hf.get(rids[5]) is None
    assert sum(1 for _ in hf.scan()) == 299
    disk.close(); shutil.rmtree(d)


def test_bplus_tree():
    t = BPlusTree(order=4)
    ref = {}
    ks = list(range(1, 1001)); random.shuffle(ks)
    for k in ks:
        t.insert(k, k * 2); ref[k] = k * 2
    assert all(t.search(k) == ref[k] for k in ref)
    assert [k for k, _ in t.range_scan(10, 20)] == list(range(10, 21))
    for k in random.sample(ks, 400):
        assert t.delete(k); del ref[k]
    assert all(t.search(k) == ref.get(k) for k in ks)


def test_sql_and_optimizer():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, name TEXT, c INT)")
    for i in range(1, 101):
        db.execute(f"INSERT INTO u VALUES ({i}, 'n{i}', {i % 4})")
    assert db.execute("SELECT id, name FROM u WHERE id = 42").rows == [(42, "n42")]
    assert "IndexScan" in db.explain("SELECT id FROM u WHERE id = 42")
    assert "SeqScan" in db.explain("SELECT id FROM u WHERE c = 2")
    assert len(db.execute("SELECT id FROM u WHERE id >= 90").rows) == 11
    db.execute("DELETE FROM u WHERE id = 42")
    assert db.execute("SELECT id FROM u WHERE id = 42").rows == []
    db.close(); shutil.rmtree(d)


def test_join():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, cid INT)")
    db.execute("CREATE TABLE c (cid INT PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO c VALUES (1, 'A')")
    db.execute("INSERT INTO c VALUES (2, 'B')")
    db.execute("INSERT INTO u VALUES (10, 1)")
    db.execute("INSERT INTO u VALUES (11, 2)")
    q = "SELECT u.id, c.name FROM u JOIN c ON u.cid = c.cid WHERE u.id = 11"
    assert db.execute(q).rows == [(11, "B")]
    db.close(); shutil.rmtree(d)


def test_recovery():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, b INT)")
    for i in range(1, 4):
        db.execute(f"INSERT INTO a VALUES ({i}, {i*10})")
    t = db.begin(); db.get_table("a").insert(t, [99, 999])     # uncommitted
    db.crash()                                                  # crash: close handles, no flush
    db2 = Database(d)
    assert db2.execute("SELECT b FROM a WHERE id = 2").rows == [(20,)]
    assert db2.execute("SELECT b FROM a WHERE id = 99").rows == []
    db2.close(); shutil.rmtree(d)


def test_crash_is_terminal_but_safe_to_close():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, b INT)")
    db.execute("INSERT INTO a VALUES (1, 10)")
    db.crash()
    db.close()
    try:
        db.checkpoint()
        assert False, "checkpoint after crash should fail clearly"
    except RuntimeError as e:
        assert "crashed" in str(e)
    shutil.rmtree(d)


def test_sql_tables_use_lsm_storage_by_default():
    d = temp_db_dir()
    db = Database(d, lsm_memtable_limit=2)
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, name TEXT, c INT)")
    for i in range(1, 6):
        db.execute(f"INSERT INTO u VALUES ({i}, 'n{i}', {i % 2})")
    assert db.get_table("u").storage_kind == "lsm"
    assert db.execute("SELECT id, name FROM u WHERE id = 4").rows == [(4, "n4")]
    assert "IndexScan" in db.explain("SELECT id FROM u WHERE id = 4")
    assert sorted(db.execute("SELECT id FROM u WHERE id >= 3").rows) == [(3,), (4,), (5,)]
    db.execute("DELETE FROM u WHERE id = 2")
    assert db.execute("SELECT id FROM u WHERE id = 2").rows == []
    db.checkpoint()
    db.close()

    db2 = Database(d)
    assert db2.get_table("u").storage_kind == "lsm"
    assert sorted(db2.execute("SELECT id FROM u WHERE c = 1").rows) == [(1,), (3,), (5,)]
    db2.close(); shutil.rmtree(d)


def test_checkpoint_rejects_active_transactions():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, b INT)")
    txn = db.begin()
    db.get_table("a").insert(txn, [1, 10])
    try:
        db.checkpoint()
        assert False, "checkpoint should reject active loser transactions"
    except RuntimeError as e:
        assert "active transaction" in str(e)
    db.abort(txn)
    db.close(); shutil.rmtree(d)


def test_table_storage_kind_survives_reopen():
    heap_dir = temp_db_dir()
    heap_db = Database(heap_dir, storage_kind="heap")
    heap_db.execute("CREATE TABLE h (id INT PRIMARY KEY, v INT)")
    heap_db.execute("INSERT INTO h VALUES (1, 10)")
    heap_db.checkpoint()
    heap_db.close()

    heap_reopen = Database(heap_dir)
    assert heap_reopen.get_table("h").storage_kind == "heap"
    assert heap_reopen.execute("SELECT v FROM h WHERE id = 1").rows == [(10,)]
    heap_reopen.close(); shutil.rmtree(heap_dir)

    lsm_dir = temp_db_dir()
    lsm_db = Database(lsm_dir)
    lsm_db.execute("CREATE TABLE l (id INT PRIMARY KEY, v INT)")
    lsm_db.execute("INSERT INTO l VALUES (1, 20)")
    lsm_db.checkpoint()
    lsm_db.close()

    lsm_reopen = Database(lsm_dir, storage_kind="heap")
    assert lsm_reopen.get_table("l").storage_kind == "lsm"
    assert lsm_reopen.execute("SELECT v FROM l WHERE id = 1").rows == [(20,)]
    lsm_reopen.close(); shutil.rmtree(lsm_dir)


def test_public_operations_fail_clearly_after_crash():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, b INT)")
    db.crash()
    for op in (lambda: db.begin(),
               lambda: db.execute("SELECT * FROM a"),
               lambda: db.explain("SELECT * FROM a")):
        try:
            op()
            assert False, "post-crash operation should fail clearly"
        except RuntimeError as e:
            assert "crashed" in str(e)
    db.close(); shutil.rmtree(d)


def test_stale_table_handles_fail_clearly_after_crash():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, b INT)")
    txn = db.begin()
    table = db.get_table("a")
    db.crash()
    for op in (lambda: list(table.seq_scan(txn)),
               lambda: table.insert(txn, [1, 10]),
               lambda: table.get_by_key(txn, 1)):
        try:
            op()
            assert False, "stale table handle should fail clearly"
        except RuntimeError as e:
            assert "crashed" in str(e)
    db.close(); shutil.rmtree(d)


def test_close_rejects_active_transactions():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, b INT)")
    txn = db.begin()
    db.get_table("a").insert(txn, [999, 999])
    try:
        db.close()
        assert False, "close should reject active transactions"
    except RuntimeError as e:
        assert "active transaction" in str(e)
    db.abort(txn)
    db.close()

    reopened = Database(d)
    assert reopened.execute("SELECT b FROM a WHERE id = 999").rows == []
    reopened.close(); shutil.rmtree(d)


def test_lsm_compaction_persists_manifest_before_deleting_inputs():
    from minidb.lsm import lsm_engine as lsm_mod

    d = temp_db_dir()
    lsm = LSMEngine(d, memtable_limit=2)
    for i in range(8):
        lsm.put(i, f"v{i}".encode())
    original = lsm_mod.SSTable.remove_files

    def fail_remove(self):
        raise RuntimeError("crash during obsolete SSTable cleanup")

    lsm_mod.SSTable.remove_files = fail_remove
    try:
        try:
            lsm.compact()
            assert False, "fault injection should stop compaction cleanup"
        except RuntimeError as e:
            assert "obsolete SSTable cleanup" in str(e)
    finally:
        lsm_mod.SSTable.remove_files = original

    reopened = LSMEngine(d)
    for i in range(8):
        assert reopened.get(i) == f"v{i}".encode()
    shutil.rmtree(d)


def test_deadlock_detection():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE k (id INT PRIMARY KEY, v INT)")
    db.execute("INSERT INTO k VALUES (1, 0)")
    db.execute("INSERT INTO k VALUES (2, 0)")
    results = {}
    barrier = threading.Barrier(2)

    def worker(name, k1, k2):
        txn = db.begin(); tbl = db.get_table("k")
        try:
            tbl.update_by_key(txn, k1, [k1, 1]); barrier.wait(); time.sleep(0.05)
            tbl.update_by_key(txn, k2, [k2, 2]); db.commit(txn)
            results[name] = "C"
        except DeadlockError:
            db.abort(txn); results[name] = "A"

    a = threading.Thread(target=worker, args=("T1", 1, 2))
    b = threading.Thread(target=worker, args=("T2", 2, 1))
    a.start(); b.start(); a.join(); b.join()
    assert "A" in results.values() and "C" in results.values()
    db.close(); shutil.rmtree(d)


def test_scan_blocks_phantom_inserts_until_commit():
    for storage_kind in ("heap", "lsm"):
        d = temp_db_dir()
        db = Database(d, storage_kind=storage_kind)
        db.execute("CREATE TABLE p (id INT PRIMARY KEY, v INT)")
        db.execute("INSERT INTO p VALUES (1, 10)")
        t1 = db.begin()
        table = db.get_table("p")
        assert list(table.seq_scan(t1)) == [(1, 10)]

        inserted = threading.Event()

        def writer():
            t2 = db.begin()
            table.insert(t2, [2, 20])
            db.commit(t2)
            inserted.set()

        thread = threading.Thread(target=writer)
        thread.start()
        time.sleep(0.05)
        assert not inserted.is_set()
        assert list(table.seq_scan(t1)) == [(1, 10)]
        db.commit(t1)
        thread.join(2)
        assert inserted.is_set()
        assert sorted(db.execute("SELECT id FROM p").rows) == [(1,), (2,)]
        db.close(); shutil.rmtree(d)


def test_index_range_blocks_phantom_inserts_until_commit():
    for storage_kind in ("heap", "lsm"):
        d = temp_db_dir()
        db = Database(d, storage_kind=storage_kind)
        db.execute("CREATE TABLE p (id INT PRIMARY KEY, v INT)")
        db.execute("INSERT INTO p VALUES (1, 10)")
        db.execute("INSERT INTO p VALUES (3, 30)")
        t1 = db.begin()
        table = db.get_table("p")
        assert list(table.index_range(t1, 1, 3)) == [(1, 10), (3, 30)]

        inserted = threading.Event()

        def writer():
            t2 = db.begin()
            table.insert(t2, [2, 20])
            db.commit(t2)
            inserted.set()

        thread = threading.Thread(target=writer)
        thread.start()
        time.sleep(0.05)
        assert not inserted.is_set()
        assert list(table.index_range(t1, 1, 3)) == [(1, 10), (3, 30)]
        db.commit(t1)
        thread.join(2)
        assert inserted.is_set()
        assert sorted(db.execute("SELECT id FROM p").rows) == [(1,), (2,), (3,)]
        db.close(); shutil.rmtree(d)


def test_wal_crash_close_drops_unflushed_records():
    d = temp_db_dir()
    path = os.path.join(d, "wal.log")
    wal = WriteAheadLog(path)
    wal.log_begin(123)
    wal.crash_close()

    reopened = WriteAheadLog(path)
    assert list(reopened.read_records()) == []
    reopened.close(); shutil.rmtree(d)


def test_index_range_blocks_updates_until_commit():
    for storage_kind in ("heap", "lsm"):
        d = temp_db_dir()
        db = Database(d, storage_kind=storage_kind)
        db.execute("CREATE TABLE r (id INT PRIMARY KEY, v INT)")
        db.execute("INSERT INTO r VALUES (1, 10)")
        db.execute("INSERT INTO r VALUES (2, 20)")
        t1 = db.begin()
        table = db.get_table("r")
        assert list(table.index_range(t1, 1, 2)) == [(1, 10), (2, 20)]

        updated = threading.Event()

        def writer():
            t2 = db.begin()
            table.update_by_key(t2, 1, [1, 99])
            db.commit(t2)
            updated.set()

        thread = threading.Thread(target=writer)
        thread.start()
        time.sleep(0.05)
        assert not updated.is_set()
        assert list(table.index_range(t1, 1, 2)) == [(1, 10), (2, 20)]
        db.commit(t1)
        thread.join(2)
        assert updated.is_set()
        assert db.execute("SELECT v FROM r WHERE id = 1").rows == [(99,)]
        db.close(); shutil.rmtree(d)


def test_heap_index_rebuilds_after_reopen():
    d = temp_db_dir()
    db = Database(d, storage_kind="heap")
    db.execute("CREATE TABLE h (id INT PRIMARY KEY, v INT)")
    for i in range(10):
        db.execute(f"INSERT INTO h VALUES ({i}, {i * 10})")
    db.checkpoint()
    db.close()

    reopened = Database(d)
    assert reopened.get_table("h").storage_kind == "heap"
    assert "IndexScan" in reopened.explain("SELECT v FROM h WHERE id = 1")
    try:
        reopened.execute("INSERT INTO h VALUES (1, 99)")
        assert False, "reopened heap table should reject duplicate primary keys"
    except ValueError as e:
        assert "duplicate primary key" in str(e)
    assert reopened.execute("SELECT v FROM h WHERE id = 1").rows == [(10,)]
    reopened.close(); shutil.rmtree(d)


def test_heap_recovery_undoes_evicted_loser_insert():
    d = temp_db_dir()
    db = Database(d, storage_kind="heap")
    db.execute("CREATE TABLE h (id INT PRIMARY KEY, payload TEXT)")
    payload = "x" * 1000
    for i in range(300):
        db.execute(f"INSERT INTO h VALUES ({i}, '{payload}')")
    db.checkpoint()

    txn = db.begin()
    table = db.get_table("h")
    table.insert(txn, [9999, "loser"])
    assert any(row[0] == 9999 for row in table.seq_scan(txn))
    db.crash()

    reopened = Database(d)
    assert reopened.execute("SELECT payload FROM h WHERE id = 9999").rows == []
    reopened.close(); shutil.rmtree(d)


def test_heap_abort_rollback_handles_relocated_updates():
    d = temp_db_dir()
    db = Database(d, storage_kind="heap")
    db.execute("CREATE TABLE h (id INT PRIMARY KEY, payload TEXT)")
    db.execute("INSERT INTO h VALUES (1, 'seed')")
    table = db.get_table("h")

    txn = db.begin()
    table.update_by_key(txn, 1, [1, "x" * 80])
    table.update_by_key(txn, 1, [1, "y" * 160])
    db.abort(txn)
    assert db.execute("SELECT id, payload FROM h").rows == [(1, "seed")]

    txn = db.begin()
    table.insert(txn, [2, "z" * 90])
    table.update_by_key(txn, 2, [2, "w" * 150])
    db.abort(txn)
    assert sorted(db.execute("SELECT id FROM h").rows) == [(1,)]
    db.close(); shutil.rmtree(d)


def test_heap_insert_logs_before_dirty_page_can_flush():
    d = temp_db_dir()
    db = Database(d, storage_kind="heap")
    db.execute("CREATE TABLE h (id INT PRIMARY KEY, payload TEXT)")
    db.execute("INSERT INTO h VALUES (1, 'seed')")
    db.checkpoint()
    table = db.get_table("h")
    original_log_insert = db.wal.log_insert

    def fail_before_log(txn_id, table_name, key, row):
        if key == 2:
            force_buffer_eviction(db)
            raise RuntimeError("simulated WAL append failure")
        return original_log_insert(txn_id, table_name, key, row)

    db.wal.log_insert = fail_before_log
    txn = db.begin()
    try:
        table.insert(txn, [2, "unlogged_loser"])
        assert False, "fault injection should stop insert"
    except RuntimeError as e:
        assert "WAL append failure" in str(e)
    db.crash()

    reopened = Database(d)
    assert reopened.execute("SELECT payload FROM h WHERE id = 2").rows == []
    reopened.close(); shutil.rmtree(d)


def test_heap_update_logs_before_dirty_page_can_flush():
    d = temp_db_dir()
    db = Database(d, storage_kind="heap")
    db.execute("CREATE TABLE h (id INT PRIMARY KEY, payload TEXT)")
    db.execute("INSERT INTO h VALUES (1, 'seed')")
    db.checkpoint()
    table = db.get_table("h")
    original_log_update = db.wal.log_update

    def fail_before_log(txn_id, table_name, key, old, row):
        if key == 1:
            force_buffer_eviction(db)
            raise RuntimeError("simulated WAL update append failure")
        return original_log_update(txn_id, table_name, key, old, row)

    db.wal.log_update = fail_before_log
    txn = db.begin()
    try:
        table.update_by_key(txn, 1, [1, "unlogged_update"])
        assert False, "fault injection should stop update"
    except RuntimeError as e:
        assert "WAL update append failure" in str(e)
    db.crash()

    reopened = Database(d)
    assert reopened.execute("SELECT payload FROM h WHERE id = 1").rows == [("seed",)]
    reopened.close(); shutil.rmtree(d)


def test_heap_delete_logs_before_dirty_page_can_flush():
    d = temp_db_dir()
    db = Database(d, storage_kind="heap")
    db.execute("CREATE TABLE h (id INT PRIMARY KEY, payload TEXT)")
    db.execute("INSERT INTO h VALUES (1, 'seed')")
    db.checkpoint()
    table = db.get_table("h")
    original_log_delete = db.wal.log_delete

    def fail_before_log(txn_id, table_name, key, old):
        if key == 1:
            force_buffer_eviction(db)
            raise RuntimeError("simulated WAL delete append failure")
        return original_log_delete(txn_id, table_name, key, old)

    db.wal.log_delete = fail_before_log
    txn = db.begin()
    try:
        table.delete_by_key(txn, 1)
        assert False, "fault injection should stop delete"
    except RuntimeError as e:
        assert "WAL delete append failure" in str(e)
    db.crash()

    reopened = Database(d)
    assert reopened.execute("SELECT payload FROM h WHERE id = 1").rows == [("seed",)]
    reopened.close(); shutil.rmtree(d)


def test_lsm_insert_logs_before_mutating_memtable():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE l (id INT PRIMARY KEY, payload TEXT)")
    table = db.get_table("l")
    original_log_insert = db.wal.log_insert

    def fail_before_log(txn_id, table_name, key, row):
        if key == 2:
            raise RuntimeError("simulated WAL append failure")
        return original_log_insert(txn_id, table_name, key, row)

    db.wal.log_insert = fail_before_log
    txn = db.begin()
    try:
        table.insert(txn, [2, "unlogged_loser"])
        assert False, "fault injection should stop insert"
    except RuntimeError as e:
        assert "WAL append failure" in str(e)
    assert table.get_by_key(txn, 2) is None
    db.abort(txn)
    db.close(); shutil.rmtree(d)


def test_lsm_update_logs_before_mutating_memtable():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE l (id INT PRIMARY KEY, payload TEXT)")
    db.execute("INSERT INTO l VALUES (1, 'seed')")
    table = db.get_table("l")
    original_log_update = db.wal.log_update

    def fail_before_log(txn_id, table_name, key, old, row):
        if key == 1:
            raise RuntimeError("simulated WAL update append failure")
        return original_log_update(txn_id, table_name, key, old, row)

    db.wal.log_update = fail_before_log
    txn = db.begin()
    try:
        table.update_by_key(txn, 1, [1, "unlogged_update"])
        assert False, "fault injection should stop update"
    except RuntimeError as e:
        assert "WAL update append failure" in str(e)
    assert table.get_by_key(txn, 1) == (1, "seed")
    db.abort(txn)
    db.close(); shutil.rmtree(d)


def test_lsm_delete_logs_before_mutating_memtable():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE l (id INT PRIMARY KEY, payload TEXT)")
    db.execute("INSERT INTO l VALUES (1, 'seed')")
    table = db.get_table("l")
    original_log_delete = db.wal.log_delete

    def fail_before_log(txn_id, table_name, key, old):
        if key == 1:
            raise RuntimeError("simulated WAL delete append failure")
        return original_log_delete(txn_id, table_name, key, old)

    db.wal.log_delete = fail_before_log
    txn = db.begin()
    try:
        table.delete_by_key(txn, 1)
        assert False, "fault injection should stop delete"
    except RuntimeError as e:
        assert "WAL delete append failure" in str(e)
    assert table.get_by_key(txn, 1) == (1, "seed")
    db.abort(txn)
    db.close(); shutil.rmtree(d)


def test_failed_commit_is_not_durable_after_crash():
    d = temp_db_dir()
    db = Database(d)
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, v INT)")
    txn = db.begin()
    db.get_table("c").insert(txn, [1, 10])
    original_fsync = os.fsync

    def fail_commit_fsync(fd):
        raise OSError("simulated commit fsync failure")

    os.fsync = fail_commit_fsync
    try:
        try:
            db.commit(txn)
            assert False, "commit should surface fsync failure"
        except OSError as e:
            assert "commit fsync failure" in str(e)
        db.crash()
    finally:
        os.fsync = original_fsync

    reopened = Database(d)
    assert reopened.execute("SELECT v FROM c WHERE id = 1").rows == []
    reopened.close(); shutil.rmtree(d)


def test_lsm_extension():
    d = temp_db_dir()
    lsm = LSMEngine(d, memtable_limit=100)
    for i in range(2000):
        lsm.put(i, f"v{i}".encode())
    for i in range(50):
        lsm.put(i, b"UPD")
    for i in range(50, 70):
        lsm.delete(i)
    lsm.flush()
    assert lsm.get(1999) == b"v1999"
    assert lsm.get(10) == b"UPD"
    assert lsm.get(60) is None
    assert lsm.get(10_000_000) is None
    assert lsm.compactions > 0
    shutil.rmtree(d)


def test_benchmark_harness_closes_temp_files():
    from benchmarks.bench_lsm_vs_btree import run

    n, logical_bytes, results = run(200)
    assert n == 200
    assert logical_bytes > 0
    assert set(results) == {"BTree", "LSM"}


def test_benchmark_harness_cleans_up_constructor_failures():
    from benchmarks import bench_lsm_vs_btree as bench

    root = temp_db_dir()
    made = []

    class BrokenStore:
        def __init__(self, directory):
            raise RuntimeError(f"constructor failed in {directory}")

    original_store = bench.BTreeStore
    original_mkdtemp = bench._mkdtemp

    def fake_mkdtemp():
        path = tempfile.mkdtemp(dir=root)
        made.append(path)
        return path

    bench.BTreeStore = BrokenStore
    bench._mkdtemp = fake_mkdtemp
    try:
        try:
            bench.run(10)
            assert False, "benchmark constructor failure should propagate"
        except RuntimeError as e:
            assert "constructor failed" in str(e)
        assert made and not os.path.exists(made[0])
    finally:
        bench.BTreeStore = original_store
        bench._mkdtemp = original_mkdtemp
        shutil.rmtree(root)


def test_benchmark_report_uses_portable_units():
    from benchmarks.bench_lsm_vs_btree import _fmt, _report

    data = {
        "BTree": {
            "write_throughput": 1,
            "read_hit_us": 2,
            "read_miss_us": 3,
            "space_amp": 4,
            "write_amp": 5,
            "compactions": 6,
            "bloom_skips": 7,
        },
        "LSM": {
            "write_throughput": 8,
            "read_hit_us": 9,
            "read_miss_us": 10,
            "space_amp": 11,
            "write_amp": 12,
            "compactions": 13,
            "bloom_skips": 14,
        },
    }
    report = _fmt(1, 10, data)
    assert "Point read hit (us)" in report
    assert "Point read miss (us)" in report
    timing_ranges = {
        "write_throughput": {"BTree": (1, 2), "LSM": (8, 9)},
        "read_hit_us": {"BTree": (2, 3), "LSM": (9, 10)},
        "read_miss_us": {"BTree": (3, 4), "LSM": (10, 11)},
    }
    full_report = _report(1, 10, data, trials=3, timing_ranges=timing_ranges)
    assert "## Workload" in full_report
    assert "## Analysis" in full_report
    assert "Median of 3 trials" in full_report
    assert "Timing range across 3 trials" in full_report


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    random.seed(1)
    for fn in fns:
        fn()
        print(f"PASS  {fn.__name__}")
    print(f"\nALL {len(fns)} TESTS PASSED")
