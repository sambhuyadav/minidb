"""Cost-based optimizer.

Responsibilities required by the brief:
  * Selectivity estimation  -> fraction of rows a predicate keeps.
  * Scan selection          -> IndexScan vs SeqScan, chosen by estimated cost.
  * Join-order selection     -> for a two-table join, pick the cheaper of
                               (A outer, B inner) vs (B outer, A inner), and use
                               an index nested-loop join when the inner join key
                               is the inner table's primary key.

Cost model (in abstract "row-touch" units):
  * SeqScan        = n_rows                       (read every row)
  * IndexScan eq   = INDEX_PROBE (~tree height)   (one root->leaf descent)
  * IndexScan range= INDEX_PROBE + est_rows
  * IndexNLJoin    = outer_rows * INDEX_PROBE
  * BlockNLJoin    = outer_rows * inner_rows

EXPLAIN renders the chosen plan with these estimates.
"""

from ..sql.ast import Select
from ..execution.operators import SeqScan, IndexScan, Filter, NestedLoopJoin

INDEX_PROBE = 3.0          # ~ B+ tree height for our scales
EQ_SELECTIVITY = 0.2       # default for equality on a non-key column
RANGE_SELECTIVITY = 0.33   # default for a range predicate

_RANGE_OPS = ("<", "<=", ">", ">=")


class Optimizer:
    def __init__(self, db):
        self.db = db

    def plan(self, stmt):
        if isinstance(stmt, Select):
            return self._plan_select(stmt)
        return None            # CREATE/INSERT/DELETE handled by the executor

    # --- helpers ------------------------------------------------------------
    def _owning_table(self, column, table_names):
        """Resolve which table a (possibly bare) column belongs to."""
        if "." in column:
            return column.split(".", 1)[0]
        for tname in table_names:
            if column in self.db.get_table(tname).schema.col_names:
                return tname
        return table_names[0]

    def _bare(self, column):
        return column.split(".", 1)[1] if "." in column else column

    def selectivity(self, table, pred):
        schema = table.schema
        col = self._bare(pred.column)
        idx = schema.index_of(col)
        if idx == schema.pk_index:
            return (1.0 / max(1, table.stats.n_rows)) if pred.op == "=" \
                else RANGE_SELECTIVITY
        return EQ_SELECTIVITY if pred.op == "=" else RANGE_SELECTIVITY

    def _build_scan(self, table, local_preds):
        """Pick IndexScan or SeqScan for one table given its local predicates."""
        schema = table.schema
        n = max(1, table.stats.n_rows)
        pk = schema.pk_name

        eq_pred = next((p for p in local_preds
                        if self._bare(p.column) == pk and p.op == "="), None)
        range_preds = [p for p in local_preds
                       if self._bare(p.column) == pk and p.op in _RANGE_OPS]

        seq_cost = float(n)
        index_op = None
        index_cost = float("inf")
        consumed = set()

        if eq_pred is not None:
            index_op = IndexScan(table, eq=eq_pred.value)
            index_op.est_rows = 1
            index_cost = INDEX_PROBE
            consumed = {id(eq_pred)}
        elif range_preds:
            lo = hi = None
            for p in range_preds:
                if p.op in (">", ">="):
                    lo = p.value
                elif p.op in ("<", "<="):
                    hi = p.value
            est = max(1, int(n * RANGE_SELECTIVITY))
            index_op = IndexScan(table, lo=lo, hi=hi)
            index_op.est_rows = est
            index_cost = INDEX_PROBE + est
            consumed = {id(p) for p in range_preds}

        if index_op is not None and index_cost < seq_cost:
            scan = index_op
        else:
            scan = SeqScan(table)
            scan.est_rows = n
            scan.est_cost = seq_cost
            consumed = set()
        scan.est_cost = min(index_cost, seq_cost) if scan is index_op else seq_cost

        remaining = [p for p in local_preds if id(p) not in consumed]
        node = scan
        if remaining:
            node = Filter(scan, remaining)
            sel = 1.0
            for p in remaining:
                sel *= self.selectivity(table, p)
            node.est_rows = max(1, int(scan.est_rows * sel))
            node.est_cost = scan.est_cost
        return node

    # --- SELECT planning ----------------------------------------------------
    def _plan_select(self, stmt):
        table_names = [stmt.from_table] + [j.table for j in stmt.joins]
        # Partition WHERE predicates by owning table (local predicates).
        local = {t: [] for t in table_names}
        residual = []
        for p in stmt.where:
            owner = self._owning_table(p.column, table_names)
            local[owner].append(p)

        if not stmt.joins:
            t = self.db.get_table(stmt.from_table)
            return self._build_scan(t, local[stmt.from_table])

        # --- single join: choose the cheaper join order --------------------
        join = stmt.joins[0]
        a_name, b_name = stmt.from_table, join.table
        a, b = self.db.get_table(a_name), self.db.get_table(b_name)
        a_scan = self._build_scan(a, local[a_name])
        b_scan = self._build_scan(b, local[b_name])

        a_col, b_col = self._orient(join, a_name, b_name)

        plan_ab = self._make_join(a_scan, b, a_col, b_col)   # A outer, B inner
        plan_ba = self._make_join(b_scan, a, b_col, a_col)   # B outer, A inner
        best = plan_ab if plan_ab.est_cost <= plan_ba.est_cost else plan_ba

        # chain any further joins in declaration order (documented simplification)
        node = best
        for extra in stmt.joins[1:]:
            inner = self.db.get_table(extra.table)
            ac, bc = self._orient(extra, None, extra.table)
            node = self._make_join(node, inner, ac, bc)
        if residual:
            node = Filter(node, residual)
        return node

    def _orient(self, join, a_name, b_name):
        """Return (outer_col, inner_col) with inner being join.table."""
        # left_col belongs to a previously-seen table; right_col to join.table
        return join.left_col, join.right_col

    def _make_join(self, outer_op, inner_table, outer_col, inner_col):
        inner_bare = self._bare(inner_col)
        use_index = (inner_bare == inner_table.schema.pk_name)
        node = NestedLoopJoin(outer_op, inner_table, outer_col, inner_col, use_index)
        outer_rows = max(1, outer_op.est_rows)
        if use_index:
            node.est_rows = outer_rows
            node.est_cost = outer_op.est_cost + outer_rows * INDEX_PROBE
        else:
            inner_rows = max(1, inner_table.stats.n_rows)
            node.est_rows = outer_rows
            node.est_cost = outer_op.est_cost + outer_rows * inner_rows
        return node


def explain(plan) -> str:
    """Render a plan tree as indented EXPLAIN text."""
    lines = []

    def walk(node, depth):
        lines.append("  " * depth + "-> " + node.describe())
        for attr in ("child", "outer"):
            child = getattr(node, attr, None)
            if child is not None and hasattr(child, "describe"):
                walk(child, depth + 1)
    walk(plan, 0)
    return "\n".join(lines)
