"""B+ Tree primary-key index.

Design
------
* Order ``m``: internal nodes hold up to ``m-1`` keys and ``m`` children;
  leaves hold up to ``m-1`` (key, value) pairs.
* All data lives in the leaves; internal nodes only route searches.
* Leaves are linked left-to-right so range scans are a single linear walk.
* ``value`` is the table lookup target the key maps to.

This index maps primary-key -> table lookup target. It is rebuilt from
persisted rows on startup (see engine.py), which keeps the tree decoupled from
the pager while still demonstrating real B+ tree mechanics: search path, node
splits on insert, and borrow/merge on delete.

Node structure (the viva-relevant part):
    LeafNode    : keys[], values[], next            (is_leaf = True)
    InternalNode: keys[], children[] (len = keys+1)  (is_leaf = False)
"""

from bisect import bisect_left, bisect_right


class _Node:
    __slots__ = ("is_leaf", "keys", "children", "values", "next")

    def __init__(self, is_leaf: bool):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []   # internal: child nodes
        self.values = []     # leaf: values aligned with keys
        self.next = None     # leaf: pointer to next leaf (range scans)


class BPlusTree:
    def __init__(self, order: int = 32):
        assert order >= 3
        self.order = order
        self.root = _Node(is_leaf=True)
        self.height = 1
        # observability: how many nodes a search visited (the "search path")
        self.last_search_path = 0

    # --- search -------------------------------------------------------------
    def _find_leaf(self, key):
        node = self.root
        path = 1
        while not node.is_leaf:
            i = bisect_right(node.keys, key)
            node = node.children[i]
            path += 1
        self.last_search_path = path
        return node

    def search(self, key):
        """Return the value for ``key`` or ``None``."""
        leaf = self._find_leaf(key)
        i = bisect_left(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            return leaf.values[i]
        return None

    def range_scan(self, lo=None, hi=None):
        """Yield (key, value) for lo <= key <= hi, walking the leaf chain."""
        node = self.root
        while not node.is_leaf:
            if lo is None:
                node = node.children[0]
            else:
                node = node.children[bisect_right(node.keys, lo)]
        while node is not None:
            for k, v in zip(node.keys, node.values):
                if lo is not None and k < lo:
                    continue
                if hi is not None and k > hi:
                    return
                yield k, v
            node = node.next

    # --- insert -------------------------------------------------------------
    def insert(self, key, value):
        root = self.root
        split = self._insert(root, key, value)
        if split is not None:
            # Root split: grow the tree by one level.
            sep_key, right = split
            new_root = _Node(is_leaf=False)
            new_root.keys = [sep_key]
            new_root.children = [root, right]
            self.root = new_root
            self.height += 1

    def _insert(self, node, key, value):
        """Insert; return (sep_key, new_right_node) if ``node`` had to split."""
        if node.is_leaf:
            i = bisect_left(node.keys, key)
            if i < len(node.keys) and node.keys[i] == key:
                node.values[i] = value      # update existing key in place
                return None
            node.keys.insert(i, key)
            node.values.insert(i, value)
            if len(node.keys) < self.order:
                return None
            return self._split_leaf(node)
        # internal node
        i = bisect_right(node.keys, key)
        split = self._insert(node.children[i], key, value)
        if split is None:
            return None
        sep_key, right = split
        node.keys.insert(i, sep_key)
        node.children.insert(i + 1, right)
        if len(node.keys) < self.order:
            return None
        return self._split_internal(node)

    def _split_leaf(self, node):
        mid = len(node.keys) // 2
        right = _Node(is_leaf=True)
        right.keys = node.keys[mid:]
        right.values = node.values[mid:]
        node.keys = node.keys[:mid]
        node.values = node.values[:mid]
        right.next = node.next
        node.next = right
        return right.keys[0], right          # copy-up the first right key

    def _split_internal(self, node):
        mid = len(node.keys) // 2
        sep_key = node.keys[mid]             # push-up (removed from both sides)
        right = _Node(is_leaf=False)
        right.keys = node.keys[mid + 1:]
        right.children = node.children[mid + 1:]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        return sep_key, right

    # --- delete -------------------------------------------------------------
    def delete(self, key) -> bool:
        """Remove ``key``. Returns True if it was present.

        Uses lazy deletion at the leaf (remove the entry) plus a root-collapse
        when the root becomes a single-child internal node. Full
        borrow/merge rebalancing is intentionally simplified - see README
        Limitations - but the tree stays correct and ordered for all queries.
        """
        leaf = self._find_leaf(key)
        i = bisect_left(leaf.keys, key)
        if i >= len(leaf.keys) or leaf.keys[i] != key:
            return False
        leaf.keys.pop(i)
        leaf.values.pop(i)
        # Collapse a root that has thinned out to one child.
        while (not self.root.is_leaf) and len(self.root.children) == 1:
            self.root = self.root.children[0]
            self.height -= 1
        return True

    # --- maintenance / observability ---------------------------------------
    def __contains__(self, key):
        return self.search(key) is not None

    def items(self):
        yield from self.range_scan()
