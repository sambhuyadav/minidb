"""Demo: end-to-end SQL with the cost-based optimizer.

Shows the optimizer choosing IndexScan vs SeqScan and an index nested-loop join,
via EXPLAIN, then running the queries.

Run:  uv run python demos/demo_sql.py
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
    db = Database(d)
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, city_id INT)")
    db.execute("CREATE TABLE cities (cid INT PRIMARY KEY, cname TEXT)")
    for cid, n in [(1, "Mumbai"), (2, "Delhi"), (3, "Pune")]:
        db.execute(f"INSERT INTO cities VALUES ({cid}, '{n}')")
    for i in range(1, 1001):
        db.execute(f"INSERT INTO users VALUES ({i}, 'user{i}', {1 + i % 3})")

    print("=== EXPLAIN: point lookup on primary key ===")
    print(db.explain("SELECT id, name FROM users WHERE id = 500"))
    print("result:", db.execute("SELECT id, name FROM users WHERE id = 500").rows)

    print("\n=== EXPLAIN: broad filter on non-key column ===")
    print(db.explain("SELECT id FROM users WHERE city_id = 2"))

    print("\n=== EXPLAIN: index nested-loop join ===")
    q = ("SELECT users.name, cities.cname FROM users "
         "JOIN cities ON users.city_id = cities.cid WHERE users.id = 7")
    print(db.explain(q))
    print("result:", db.execute(q).rows)

    print("\n=== range scan on primary key ===")
    print("ids in [998, 1000]:",
          sorted(r[0] for r in db.execute("SELECT id FROM users WHERE id >= 998").rows))
    db.close(); shutil.rmtree(d)


if __name__ == "__main__":
    main()
