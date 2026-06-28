"""Catalog - persists table metadata.

Stored as a small JSON sidecar next to the data file. It records each table's
schema, heap page list, and storage kind. On startup the engine recreates the
right access method for every table: heap tables scan their page list, and LSM
tables reopen their SSTable manifest. Both paths rebuild the in-memory B+ tree
primary-key index from persisted rows.
"""

import json
import os

from .schema import Schema


class Catalog:
    def __init__(self, path: str):
        self.path = path
        self.tables = {}  # name -> schema, heap page ids, and storage kind
        if os.path.exists(path):
            self.load()

    def add_table(self, schema: Schema, storage_kind: str = "heap"):
        if schema.table in self.tables:
            raise ValueError(f"table {schema.table} already exists")
        self.tables[schema.table] = {
            "schema": schema,
            "page_ids": [],
            "storage_kind": storage_kind,
        }
        self.save()

    def page_ids(self, table: str) -> list:
        return self.tables[table]["page_ids"]

    def schema(self, table: str) -> Schema:
        return self.tables[table]["schema"]

    def storage_kind(self, table: str) -> str:
        return self.tables[table].get("storage_kind", "heap")

    def has(self, table: str) -> bool:
        return table in self.tables

    def save(self):
        data = {
            name: {
                "schema": t["schema"].to_dict(),
                "page_ids": t["page_ids"],
                "storage_kind": t.get("storage_kind", "heap"),
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
                "storage_kind": t.get("storage_kind", "heap"),
            }
            for name, t in data.items()
        }
