"""Catalog — persists table metadata (schema, heap page list, pk column).

Stored as a small JSON sidecar next to the data file. On startup the engine
loads the catalog, recreates each table's heap-file view over its page list, and
rebuilds the in-memory B+ tree index by scanning the heap.
"""

import json
import os

from .schema import Schema


class Catalog:
    def __init__(self, path: str):
        self.path = path
        self.tables = {}            # name -> {"schema": Schema, "page_ids": [..]}
        if os.path.exists(path):
            self.load()

    def add_table(self, schema: Schema):
        if schema.table in self.tables:
            raise ValueError(f"table {schema.table} already exists")
        self.tables[schema.table] = {"schema": schema, "page_ids": []}
        self.save()

    def page_ids(self, table: str) -> list:
        return self.tables[table]["page_ids"]

    def schema(self, table: str) -> Schema:
        return self.tables[table]["schema"]

    def has(self, table: str) -> bool:
        return table in self.tables

    def save(self):
        data = {
            name: {
                "schema": t["schema"].to_dict(),
                "page_ids": t["page_ids"],
            }
            for name, t in self.tables.items()
        }
        with open(self.path, "w") as f:
            json.dump(data, f)

    def load(self):
        with open(self.path, "r") as f:
            data = json.load(f)
        self.tables = {
            name: {
                "schema": Schema.from_dict(t["schema"]),
                "page_ids": t["page_ids"],
            }
            for name, t in data.items()
        }
