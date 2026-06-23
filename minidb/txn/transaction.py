"""Transaction object and manager.

A transaction tracks its id, state, and an in-memory undo list used to roll back
on abort. Durability is provided by the WAL (see recovery/wal.py); the undo list
here is for in-memory rollback of an aborting transaction.
"""

import itertools

ACTIVE = "ACTIVE"
COMMITTED = "COMMITTED"
ABORTED = "ABORTED"


class Transaction:
    _ids = itertools.count(1)

    def __init__(self):
        self.txn_id = next(self._ids)
        self.state = ACTIVE
        # undo entries: callables that revert an applied change in memory
        self.undo = []
        self.locks = set()

    def __repr__(self):
        return f"<Txn {self.txn_id} {self.state}>"
