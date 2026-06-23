"""Schema + row (de)serialization.

MiniDB supports two column types:
    INT  -> 8-byte little-endian signed integer
    TEXT -> 2-byte length prefix followed by UTF-8 bytes

A row is a Python tuple of values in column order. ``serialize`` turns it into
the compact byte string stored on a heap page; ``deserialize`` reverses it.
"""

import struct
from dataclasses import dataclass, field
from typing import List, Tuple

INT = "INT"
TEXT = "TEXT"

_INT = struct.Struct("<q")
_LEN = struct.Struct("<H")


@dataclass
class Column:
    name: str
    type: str  # INT or TEXT


@dataclass
class Schema:
    table: str
    columns: List[Column]
    pk_index: int = 0          # which column is the primary key

    # --- name/type lookups --------------------------------------------------
    def index_of(self, col_name: str) -> int:
        for i, c in enumerate(self.columns):
            if c.name == col_name:
                return i
        raise KeyError(f"no column {col_name!r} in {self.table}")

    @property
    def col_names(self) -> List[str]:
        return [c.name for c in self.columns]

    @property
    def pk_name(self) -> str:
        return self.columns[self.pk_index].name

    # --- serialization ------------------------------------------------------
    def serialize(self, row: Tuple) -> bytes:
        if len(row) != len(self.columns):
            raise ValueError(f"row has {len(row)} values, schema has {len(self.columns)}")
        out = bytearray()
        for value, col in zip(row, self.columns):
            if col.type == INT:
                out += _INT.pack(int(value))
            else:  # TEXT
                b = str(value).encode("utf-8")
                out += _LEN.pack(len(b)) + b
        return bytes(out)

    def deserialize(self, data: bytes) -> Tuple:
        values = []
        off = 0
        for col in self.columns:
            if col.type == INT:
                (v,) = _INT.unpack_from(data, off)
                off += _INT.size
                values.append(v)
            else:  # TEXT
                (ln,) = _LEN.unpack_from(data, off)
                off += _LEN.size
                values.append(data[off:off + ln].decode("utf-8"))
                off += ln
        return tuple(values)

    # --- (de)serialization of the schema itself, for the catalog ------------
    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "columns": [{"name": c.name, "type": c.type} for c in self.columns],
            "pk_index": self.pk_index,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Schema":
        return cls(
            table=d["table"],
            columns=[Column(c["name"], c["type"]) for c in d["columns"]],
            pk_index=d["pk_index"],
        )
