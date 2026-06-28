"""Demo: 2PL concurrency control and deadlock detection.

Two transactions lock two rows in opposite orders, creating a cycle in the
wait-for graph. The lock manager detects it and aborts one transaction (the
victim); the other proceeds and commits.

Run:  uv run python demos/demo_concurrency.py
"""

import os
import shutil
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from minidb.engine import Database
from minidb.txn.lock_manager import DeadlockError

TEMP_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         ".minidb_tmp")


def demo_dir():
    os.makedirs(TEMP_ROOT, exist_ok=True)
    return tempfile.mkdtemp(dir=TEMP_ROOT)


def main():
    d = demo_dir()
    db = Database(d)
    db.execute("CREATE TABLE acct (id INT PRIMARY KEY, bal INT)")
    db.execute("INSERT INTO acct VALUES (1, 100)")
    db.execute("INSERT INTO acct VALUES (2, 100)")

    print("=== Concurrent shared reads (should NOT block) ===")
    t1, t2 = db.begin(), db.begin()
    db.get_table("acct").get_by_key(t1, 1)
    db.get_table("acct").get_by_key(t2, 1)
    print("  both transactions hold a SHARED lock on row 1 simultaneously: OK")
    db.commit(t1); db.commit(t2)

    print("\n=== Deadlock scenario (X locks acquired in opposite order) ===")
    results = {}
    barrier = threading.Barrier(2)

    def worker(name, k1, k2):
        txn = db.begin()
        tbl = db.get_table("acct")
        try:
            tbl.update_by_key(txn, k1, [k1, 1])
            print(f"  {name}: locked row {k1}")
            barrier.wait()
            time.sleep(0.05)
            tbl.update_by_key(txn, k2, [k2, 2])    # conflicts -> wait -> cycle
            db.commit(txn)
            results[name] = "COMMITTED"
        except DeadlockError:
            db.abort(txn)
            results[name] = "ABORTED (deadlock victim)"

    a = threading.Thread(target=worker, args=("T1", 1, 2))
    b = threading.Thread(target=worker, args=("T2", 2, 1))
    a.start(); b.start(); a.join(); b.join()
    for name, r in results.items():
        print(f"  {name}: {r}")
    db.close(); shutil.rmtree(d)
    print("\nResult: cycle detected; one victim aborted, the other committed.")


if __name__ == "__main__":
    main()
