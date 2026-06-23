"""Test suite for MiniDB. Runnable with `pytest` or `python3 tests/test_minidb.py`.

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
from minidb.txn.lock_manager import DeadlockError


def test_storage_heap_and_buffer():
    d = tempfile.mkdtemp()
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
    d = tempfile.mkdtemp()
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
    d = tempfile.mkdtemp()
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
    d = tempfile.mkdtemp()
    db = Database(d)
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, b INT)")
    for i in range(1, 4):
        db.execute(f"INSERT INTO a VALUES ({i}, {i*10})")
    t = db.begin(); db.get_table("a").insert(t, [99, 999])     # uncommitted
    del db                                                      # crash
    db2 = Database(d)
    assert db2.execute("SELECT b FROM a WHERE id = 2").rows == [(20,)]
    assert db2.execute("SELECT b FROM a WHERE id = 99").rows == []
    db2.close(); shutil.rmtree(d)


def test_deadlock_detection():
    d = tempfile.mkdtemp()
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


def test_lsm_extension():
    d = tempfile.mkdtemp()
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


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    random.seed(1)
    for fn in fns:
        fn()
        print(f"PASS  {fn.__name__}")
    print(f"\nALL {len(fns)} TESTS PASSED")
