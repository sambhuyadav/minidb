"""MiniDB - a small but complete relational database engine.

Layers (bottom to top):
    storage   -> page format, disk manager, buffer pool, heap files
    index     -> B+ tree (primary key index)
    lsm       -> LSM-tree storage engine (Extension Track C)
    catalog   -> table schemas / metadata
    sql       -> tokenizer + parser
    optimizer -> cost-based plan selection
    execution -> physical operators + executor
    txn       -> lock manager + 2PL transactions + deadlock detection
    recovery  -> write-ahead logging + crash recovery
    engine    -> Database facade tying everything together
"""

__version__ = "1.0.0"
