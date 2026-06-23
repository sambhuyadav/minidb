"""Write-Ahead Log (WAL) and crash recovery.

Durability rule (WAL invariant): a log record describing a change is forced to
stable storage *before* the transaction is allowed to report COMMIT. Data pages
themselves are written lazily (NO-FORCE) and uncommitted changes are kept out of
the data file (NO-STEAL, enforced by flushing only at checkpoint). Under that
policy, crash recovery is pure **redo**:

    1. Scan the log; a transaction is a winner iff it has a COMMIT record.
    2. Replay (redo) the operations of winner transactions in log order.
    3. Losers (no COMMIT) are ignored — their effects never reached the data
       file, so there is nothing to undo.

Log records are newline-delimited JSON so they can be inspected during the demo.
"""

import json
import os


class WriteAheadLog:
    def __init__(self, path: str):
        self.path = path
        if not os.path.exists(path):
            open(path, "w").close()
        self._f = open(path, "a+")
        self._lsn = 0

    def _append(self, record: dict) -> int:
        self._lsn += 1
        record["lsn"] = self._lsn
        self._f.write(json.dumps(record) + "\n")
        return self._lsn

    # --- log-record producers ----------------------------------------------
    def log_begin(self, txn_id):
        self._append({"type": "BEGIN", "txn": txn_id})

    def log_insert(self, txn_id, table, key, row):
        self._append({"type": "INSERT", "txn": txn_id, "table": table,
                      "key": key, "row": list(row)})

    def log_update(self, txn_id, table, key, old, row):
        self._append({"type": "UPDATE", "txn": txn_id, "table": table,
                      "key": key, "old": list(old), "row": list(row)})

    def log_delete(self, txn_id, table, key, old):
        self._append({"type": "DELETE", "txn": txn_id, "table": table,
                      "key": key, "old": list(old)})

    def log_commit(self, txn_id):
        self._append({"type": "COMMIT", "txn": txn_id})
        self.flush()                      # WAL invariant: force log on commit

    def log_abort(self, txn_id):
        self._append({"type": "ABORT", "txn": txn_id})

    def log_checkpoint(self):
        self._append({"type": "CHECKPOINT"})
        self.flush()

    # --- durability ---------------------------------------------------------
    def flush(self):
        self._f.flush()
        os.fsync(self._f.fileno())

    def truncate(self):
        """Reset the log (called after a checkpoint flushes all data)."""
        self._f.close()
        self._f = open(self.path, "w")
        self.flush()
        self._lsn = 0

    def close(self):
        try:
            self._f.flush()
            self._f.close()
        except Exception:
            pass

    # --- reading / recovery -------------------------------------------------
    def read_records(self):
        with open(self.path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)


def recover(wal: WriteAheadLog, apply_fn):
    """Run pure-redo recovery. ``apply_fn(record)`` redoes one data operation.

    Returns a small report dict for the demo.
    """
    records = list(wal.read_records())
    # Winners are transactions with a COMMIT after the last checkpoint.
    last_ckpt = max((i for i, r in enumerate(records)
                     if r["type"] == "CHECKPOINT"), default=-1)
    tail = records[last_ckpt + 1:]
    committed = {r["txn"] for r in tail if r["type"] == "COMMIT"}
    redone = 0
    for r in tail:
        if r["type"] in ("INSERT", "UPDATE", "DELETE") and r["txn"] in committed:
            apply_fn(r)
            redone += 1
    losers = {r["txn"] for r in tail
              if r["type"] == "BEGIN" and r["txn"] not in committed}
    return {
        "records_scanned": len(tail),
        "committed_txns": sorted(committed),
        "loser_txns": sorted(losers),
        "operations_redone": redone,
    }
