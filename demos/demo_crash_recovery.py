"""Demo: WAL-based crash recovery.

Shows that committed transactions survive a crash (redone from the WAL) while an
uncommitted transaction's changes are discarded.

Run:  uv run python demos/demo_crash_recovery.py
"""

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from minidb.engine import Database

TEMP_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         ".minidb_tmp")


def demo_dir():
    os.makedirs(TEMP_ROOT, exist_ok=True)
    return tempfile.mkdtemp(dir=TEMP_ROOT)


def main():
    d = demo_dir()
    print("=== Session 1: write committed rows, then CRASH ===")
    db = Database(d)
    db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)")
    for i, bal in [(1, 100), (2, 200), (3, 300)]:
        db.execute(f"INSERT INTO accounts VALUES ({i}, {bal})")
        print(f"  committed: account {i} = {bal}")

    t = db.begin()
    db.get_table("accounts").insert(t, [99, 9999])
    print("  account 99 written but NOT committed")
    print("  *** CRASH (process dies; buffer pool + uncommitted change lost) ***")
    db.crash()                   # close handles only; no checkpoint or page flush

    print("\n=== Session 2: reopen -> automatic recovery ===")
    db2 = Database(d)
    print("  recovery report:", db2.last_recovery)
    for i in (1, 2, 3, 99):
        rows = db2.execute(f"SELECT id, balance FROM accounts WHERE id = {i}").rows
        verdict = rows if rows else "GONE (uncommitted)"
        print(f"  account {i}: {verdict}")
    db2.close()
    shutil.rmtree(d)
    print("\nResult: committed data durable via WAL; uncommitted correctly lost.")


if __name__ == "__main__":
    main()
