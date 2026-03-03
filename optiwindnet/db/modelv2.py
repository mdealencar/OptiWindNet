# SPDX-License-Identifier: MIT
# https://gitlab.windenergy.dtu.dk/TOPFARM/OptiWindNet/
"""Database model v2 structures for storage of locations and route sets.

This module defines the schema-level table/record abstractions used by the
awkward/parquet backend:
  - NodeSet: location definition
  - RouteSet: routeset (i.e. a record of G)
  - Method: info on algorithm & options to produce routesets
  - Machine: info on machine that generated a routeset
"""

from dataclasses import dataclass
from typing import Any

import awkward as ak

__all__ = ('ParquetDatabase', 'create_empty_data', 'open_database')


@dataclass
class _Record:
    _db: 'ParquetDatabase'
    _row: dict[str, Any]

    def __getattr__(self, item: str) -> Any:
        if item not in self._row:
            if item == 'RouteSets':
                if 'B' in self._row and 'digest' in self._row:
                    digest = self._row['digest']
                    return self._db.RouteSet.select(lambda rs: rs.nodes.digest == digest)
                if 'solver_name' in self._row and 'digest' in self._row:
                    digest = self._row['digest']
                    return self._db.RouteSet.select(lambda rs: rs.method.digest == digest)
                if 'name' in self._row and 'id' in self._row:
                    machine_id = int(self._row['id'])
                    return self._db.RouteSet.select(lambda rs: rs.machine and rs.machine.id == machine_id)
            return None
        value = self._row[item]
        if item in {'nodes', 'method'} and isinstance(
            value, (bytes, bytearray, memoryview)
        ):
            table = self._db.NodeSet if item == 'nodes' else self._db.Method
            return table[bytes(value)]
        if item == 'machine' and value is not None:
            return self._db.Machine[int(value)]
        if item == 'timestamp' and isinstance(value, str):
            import datetime

            return datetime.datetime.fromisoformat(value)
        return value

    def get_pk(self):
        return self._row.get('id')


class _Table:
    def __init__(
        self, db: 'ParquetDatabase', name: str, key_field: str, key_codec=None
    ):
        self.db = db
        self.name = name
        self.key_field = key_field
        self.key_codec = key_codec or (lambda x: x)

    @property
    def _rows(self):
        return self.db._data[self.name]

    def __getitem__(self, key):
        if isinstance(key, tuple):
            key = key[0]
        skey = self.key_codec(key)
        for row in self._rows:
            if row[self.key_field] == skey:
                return _Record(self.db, row)
        raise KeyError(key)

    def exists(self, **kwargs):
        return self.get(**kwargs) is not None

    def get(self, predicate=None, **kwargs):
        if predicate is not None and callable(predicate):
            for row in self._rows:
                rec = _Record(self.db, row)
                if predicate(rec):
                    return rec
            return None
        for row in self._rows:
            if all(row.get(k) == v for k, v in kwargs.items()):
                return _Record(self.db, row)
        return None

    def select(self, predicate=None):
        records = [_Record(self.db, row) for row in self._rows]
        if predicate is None:
            return records
        return [r for r in records if predicate(r)]

    def add(self, row: dict[str, Any]):
        self._rows.append(row)
        if self.db.autosave:
            self.db.save()
        return _Record(self.db, row)


class ParquetDatabase:
    """Schema container for awkward/parquet-backed OptiWindNet database."""

    def __init__(self, filepath: str, data: dict[str, Any], autosave: bool = True):
        self.filepath = filepath
        self._data = data
        self.autosave = autosave
        self.Entity = object
        self.NodeSet = _Table(self, 'nodesets', 'digest', key_codec=bytes)
        self.Method = _Table(self, 'methods', 'digest', key_codec=bytes)
        self.Machine = _Table(self, 'machines', 'id', key_codec=int)
        self.RouteSet = _Table(self, 'routesets', 'id', key_codec=int)
        self.entities = {
            'NodeSet': self.NodeSet,
            'Method': self.Method,
            'Machine': self.Machine,
            'RouteSet': self.RouteSet,
        }

    def _next_id(self, table: str):
        self._data['next_ids'][table] += 1
        self.save()
        return self._data['next_ids'][table] - 1

    def save(self):
        ak.to_parquet(ak.Array([self._data]), self.filepath)

    def flush(self):
        self.save()


def create_empty_data() -> dict[str, Any]:
    """Return an empty database payload for schema version 2."""
    return {
        'schema_version': 2,
        'nodesets': [],
        'methods': [],
        'machines': [],
        'routesets': [],
        'next_ids': {'machines': 1, 'routesets': 1},
    }


def open_database(filepath: str, create_db: bool = False):
    """Compatibility wrapper that delegates to storagev2.open_database."""
    from .storagev2 import open_database as _open_database

    return _open_database(filepath=filepath, create_db=create_db)
