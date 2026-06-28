"""Abstract syntax tree node types produced by the parser."""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Any


@dataclass
class Predicate:
    """A single comparison: column OP value (column may be 'table.col')."""
    column: str
    op: str            # one of = != < <= > >=
    value: Any


@dataclass
class JoinClause:
    table: str
    left_col: str      # qualified column from the left side
    right_col: str     # qualified column from the joined table


@dataclass
class CreateTable:
    table: str
    columns: List[Tuple[str, str]]      # (name, type)
    pk_index: int


@dataclass
class Insert:
    table: str
    values: List[Any]


@dataclass
class Select:
    columns: List[str]                  # ['*'] or qualified/bare names
    from_table: str
    joins: List[JoinClause] = field(default_factory=list)
    where: List[Predicate] = field(default_factory=list)   # AND-ed predicates


@dataclass
class Delete:
    table: str
    where: List[Predicate] = field(default_factory=list)
