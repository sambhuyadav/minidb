"""Executor - turns a parsed statement (+ optimizer plan) into results.

DML/DDL (CREATE/INSERT/DELETE) are applied directly through the transactional
Table API. SELECT drives the operator tree produced by the optimizer and then
projects the requested columns.
"""

from ..sql.ast import CreateTable, Insert, Select, Delete
from ..catalog.schema import Schema, Column
from .operators import resolve, eval_predicate, SeqScan, IndexScan


class Result:
    def __init__(self, columns=None, rows=None, message=None, rowcount=None):
        self.columns = columns or []
        self.rows = rows or []
        self.message = message
        self.rowcount = rowcount

    def __repr__(self):
        if self.message:
            return self.message
        head = " | ".join(self.columns)
        body = "\n".join(" | ".join(str(v) for v in r) for r in self.rows)
        return f"{head}\n{'-' * len(head)}\n{body}" if self.rows else f"{head}\n(0 rows)"


class Executor:
    def __init__(self, db, txn):
        self.db = db
        self.txn = txn

    def run(self, plan, stmt):
        if isinstance(stmt, CreateTable):
            return self._create(stmt)
        if isinstance(stmt, Insert):
            return self._insert(stmt)
        if isinstance(stmt, Delete):
            return self._delete(stmt)
        if isinstance(stmt, Select):
            return self._select(plan, stmt)
        raise ValueError(f"cannot execute {stmt}")

    def _create(self, stmt: CreateTable):
        schema = Schema(stmt.table,
                        [Column(n, t) for n, t in stmt.columns],
                        pk_index=stmt.pk_index)
        self.db.create_table(schema)
        return Result(message=f"table {stmt.table} created")

    def _insert(self, stmt: Insert):
        table = self.db.get_table(stmt.table)
        table.insert(self.txn, stmt.values)
        return Result(message="1 row inserted", rowcount=1)

    def _delete(self, stmt: Delete):
        table = self.db.get_table(stmt.table)
        pk = table.schema.pk_index
        # Collect matching primary keys first, then delete (avoid mutating mid-scan).
        keys = []
        for tup in table.seq_scan(self.txn):
            row = {c: v for c, v in zip(table.schema.col_names, tup)}
            row.update({f"{table.name}.{c}": v
                        for c, v in zip(table.schema.col_names, tup)})
            if all(eval_predicate(row, p) for p in stmt.where):
                keys.append(tup[pk])
        for k in keys:
            table.delete_by_key(self.txn, k)
        return Result(message=f"{len(keys)} row(s) deleted", rowcount=len(keys))

    def _select(self, plan, stmt: Select):
        rows = list(plan.rows(self.txn))
        out_cols = self._output_columns(stmt)
        out_rows = [tuple(resolve(r, c) for c in out_cols) for r in rows]
        return Result(columns=out_cols, rows=out_rows, rowcount=len(out_rows))

    def _output_columns(self, stmt: Select):
        if stmt.columns == ["*"]:
            tables = [stmt.from_table] + [j.table for j in stmt.joins]
            if len(tables) == 1:
                return self.db.get_table(stmt.from_table).schema.col_names
            cols = []
            for t in tables:
                cols += [f"{t}.{c}" for c in self.db.get_table(t).schema.col_names]
            return cols
        return stmt.columns
