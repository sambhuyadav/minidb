"""Physical operators — Volcano-style iterators.

Each operator yields *rows*, where a row is a dict keyed by both the qualified
name ``table.col`` and (when unambiguous) the bare ``col``. This uniform shape
lets filters, joins and projections resolve columns the same way regardless of
how many tables are involved.

Operators carry ``est_rows``/``est_cost`` (filled in by the optimizer) and a
``describe()`` used to render EXPLAIN output.
"""


def _row_dict(table_name, col_names, values):
    row = {}
    for c, v in zip(col_names, values):
        row[f"{table_name}.{c}"] = v
        row.setdefault(c, v)               # bare name (first writer wins)
    return row


def resolve(row, column):
    """Look up a column reference (qualified or bare) in a row dict."""
    if column in row:
        return row[column]
    raise KeyError(f"column {column!r} not found in row {list(row)}")


def eval_predicate(row, pred):
    left = resolve(row, pred.column)
    right = pred.value
    op = pred.op
    if op == "=":
        return left == right
    if op == "!=":
        return left != right
    if op == "<":
        return left < right
    if op == "<=":
        return left <= right
    if op == ">":
        return left > right
    if op == ">=":
        return left >= right
    raise ValueError(f"unknown operator {op}")


class Operator:
    est_rows = 0
    est_cost = 0.0

    def rows(self, txn):
        raise NotImplementedError

    def describe(self):
        return self.__class__.__name__


class SeqScan(Operator):
    def __init__(self, table):
        self.table = table

    def rows(self, txn):
        cn = self.table.schema.col_names
        for tup in self.table.seq_scan(txn):
            yield _row_dict(self.table.name, cn, tup)

    def describe(self):
        return (f"SeqScan on {self.table.name} "
                f"(est_rows={self.est_rows}, cost={self.est_cost:.1f})")


class IndexScan(Operator):
    """Primary-key index scan: equality point lookup or [lo, hi] range."""
    def __init__(self, table, eq=None, lo=None, hi=None):
        self.table = table
        self.eq = eq
        self.lo = lo
        self.hi = hi

    def rows(self, txn):
        cn = self.table.schema.col_names
        if self.eq is not None:
            val = self.table.get_by_key(txn, self.eq)
            if val is not None:
                yield _row_dict(self.table.name, cn, val)
        else:
            for tup in self.table.index_range(txn, self.lo, self.hi):
                yield _row_dict(self.table.name, cn, tup)

    def describe(self):
        cond = (f"pk={self.eq}" if self.eq is not None
                else f"pk in [{self.lo},{self.hi}]")
        return (f"IndexScan on {self.table.name} ({cond}) "
                f"(est_rows={self.est_rows}, cost={self.est_cost:.1f})")


class Filter(Operator):
    def __init__(self, child, predicates):
        self.child = child
        self.predicates = predicates

    def rows(self, txn):
        for row in self.child.rows(txn):
            if all(eval_predicate(row, p) for p in self.predicates):
                yield row

    def describe(self):
        conds = ", ".join(f"{p.column}{p.op}{p.value}" for p in self.predicates)
        return f"Filter({conds})"


class NestedLoopJoin(Operator):
    """Join outer rows to a base inner table.

    If ``use_index`` is set, the inner join column is the inner table's primary
    key, so each outer row probes the B+ tree (index nested-loop join).
    Otherwise the inner table is scanned and filtered (block nested-loop).
    """
    def __init__(self, outer, inner_table, outer_col, inner_col, use_index):
        self.outer = outer
        self.inner_table = inner_table
        self.outer_col = outer_col
        self.inner_col = inner_col
        self.use_index = use_index

    def rows(self, txn):
        cn = self.inner_table.schema.col_names
        if not self.use_index:
            inner_rows = [_row_dict(self.inner_table.name, cn, t)
                          for t in self.inner_table.seq_scan(txn)]
        for orow in self.outer.rows(txn):
            key = resolve(orow, self.outer_col)
            if self.use_index:
                ival = self.inner_table.get_by_key(txn, key)
                matches = ([_row_dict(self.inner_table.name, cn, ival)]
                           if ival is not None else [])
            else:
                matches = [ir for ir in inner_rows
                           if resolve(ir, self.inner_col) == key]
            for irow in matches:
                merged = dict(orow)
                merged.update(irow)
                yield merged

    def describe(self):
        kind = "IndexNestedLoop" if self.use_index else "BlockNestedLoop"
        return (f"{kind}Join({self.outer_col}={self.inner_col}) "
                f"(est_rows={self.est_rows}, cost={self.est_cost:.1f})")
