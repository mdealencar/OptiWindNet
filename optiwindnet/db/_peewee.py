# SPDX-License-Identifier: MIT

import json
import os
import pickle
import re
from types import SimpleNamespace
from typing import Any

from peewee import AutoField, BlobField, BooleanField, DateTimeField, FloatField, ForeignKeyField, IntegerField, Model, SqliteDatabase, TextField


class IntArrayField(TextField):
    field_type = 'INT'
    def db_value(self, value):
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray, memoryview, str)):
            return value
        return json.dumps([int(v) for v in value], separators=(',', ':'))

    def python_value(self, value):
        if value is None or isinstance(value, list):
            return value
        if isinstance(value, memoryview):
            value = value.tobytes()
        if isinstance(value, (bytes, bytearray)):
            for decoder in (self._try_pickle, self._try_utf8_json, self._try_utf8_split):
                out = decoder(value)
                if out is not None:
                    return out
            return []
        if isinstance(value, str):
            for decoder in (self._parse_json, self._parse_split):
                out = decoder(value)
                if out is not None:
                    return out
        return value

    def _try_pickle(self, value):
        try:
            obj = pickle.loads(value)
            if hasattr(obj, 'tolist'):
                obj = obj.tolist()
            return [int(v) for v in obj]
        except Exception:
            return None

    def _try_utf8_json(self, value):
        try:
            return self._parse_json(value.decode())
        except Exception:
            return None

    def _try_utf8_split(self, value):
        try:
            return self._parse_split(value.decode())
        except Exception:
            return None

    def _parse_json(self, value: str):
        try:
            obj = json.loads(value)
            return [int(v) for v in obj]
        except Exception:
            return None

    def _parse_split(self, value: str):
        s = value.strip()
        if not s:
            return []
        parts = [p for p in re.split(r'[\s,;]+', s.strip('[]()')) if p]
        try:
            return [int(p) for p in parts]
        except ValueError:
            return None


class JsonCompatField(TextField):
    field_type = 'JSON'
    def db_value(self, value):
        if value is None:
            return None
        if isinstance(value, (str, bytes, bytearray, memoryview)):
            return value
        return json.dumps(value, separators=(',', ':'))

    def python_value(self, value):
        if value is None or isinstance(value, (dict, list)):
            return value
        if isinstance(value, memoryview):
            value = value.tobytes()
        if isinstance(value, (bytes, bytearray)):
            try:
                value = value.decode()
            except Exception:
                try:
                    return pickle.loads(value)
                except Exception:
                    return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return value
        return value


def open_sqlite_database(filepath: str, create_db: bool = False) -> SqliteDatabase:
    path = os.path.abspath(os.path.expanduser(str(filepath)))
    if not create_db and not os.path.exists(path):
        raise OSError(f'No such file: {path}')
    db = SqliteDatabase(path)
    db.connect(reuse_if_open=True)
    return db


def make_db_namespace(database: SqliteDatabase, **models: type[Model]) -> Any:
    ns = SimpleNamespace(database=database, Entity=Model, entities=models)
    for name, model in models.items():
        setattr(ns, name, model)
    ns.close = database.close
    return ns


__all__ = [
    'AutoField',
    'BlobField',
    'BooleanField',
    'DateTimeField',
    'FloatField',
    'ForeignKeyField',
    'IntArrayField',
    'IntegerField',
    'JsonCompatField',
    'Model',
    'TextField',
    'make_db_namespace',
    'open_sqlite_database',
]
