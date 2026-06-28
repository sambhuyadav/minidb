"""Interactive REPL for MiniDB.

Usage:  uv run minidb [data_dir]
        uv run python -m minidb.cli [data_dir]

Commands:
    <SQL>;            run a SQL statement (CREATE / INSERT / SELECT / DELETE)
    EXPLAIN <SELECT>; show the optimizer's chosen plan with cost estimates
    .tables           list tables
    .stats            buffer pool + table statistics
    .checkpoint       flush dirty pages and truncate the WAL
    .help / .exit
"""

import sys

from .engine import Database


BANNER = "MiniDB 1.0 - type .help for commands, .exit to quit"


def repl(directory="minidb_data"):
    db = Database(directory)
    print(BANNER)
    print(f"(data dir: {directory}; recovery: {db.last_recovery})")
    try:
        while True:
            try:
                line = input("minidb> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not line:
                continue
            if line in (".exit", ".quit"):
                break
            if line == ".help":
                print(__doc__)
                continue
            if line == ".tables":
                print(", ".join(db.tables) or "(no tables)")
                continue
            if line == ".stats":
                print("buffer pool:", db.bp.stats())
                for name, t in db.tables.items():
                    print(f"  {name}: rows~{t.stats.n_rows}, "
                          f"pk_range=[{t.stats.min_key},{t.stats.max_key}], "
                          f"index_height={t.index.height}")
                continue
            if line == ".checkpoint":
                db.checkpoint()
                print("checkpoint done (pages flushed, WAL truncated)")
                continue
            try:
                if line.lower().startswith("explain "):
                    print(db.explain(line[len("explain "):]))
                else:
                    print(db.execute(line))
            except Exception as e:
                print(f"error: {e}")
    finally:
        db.close()
        print("bye.")


def main():
    repl(sys.argv[1] if len(sys.argv) > 1 else "minidb_data")


if __name__ == "__main__":
    main()
