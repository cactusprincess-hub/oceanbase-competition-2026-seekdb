# Copyright (c) 2021 OceanBase
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shadow-paged, immutable bucket snapshots with an atomically published catalog.

The catalog is the commit record. Unpublished files are never visible after a
restart. No database engine, external KV store, or in-memory whole-table index
is used. Only the catalog and a bounded page cache remain resident.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import struct
import uuid
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

import numpy as np

from .errors import DatabaseError

_U32 = struct.Struct('<I')
_I64 = struct.Struct('<q')
BUCKETS = 256


def bucket(key: int) -> int:
    # SplitMix64 prevents sequential or low-bit-correlated user keys from
    # concentrating in one partition. Negative signed BIGINT keys are valid.
    value = (key + 0x9E3779B97F4A7C15) & ((1 << 64) - 1)
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & ((1 << 64) - 1)
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & ((1 << 64) - 1)
    return (value ^ (value >> 31)) % BUCKETS


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class RowView(Mapping):
    def __init__(self, store: Store, metadata: dict):
        self.store, self.metadata = store, metadata

    def __getitem__(self, key: int) -> dict:
        for row in self.store.scan_bucket(self.metadata, bucket(key)):
            if row[self.metadata['primary_key']] == key:
                return row
        raise KeyError(key)

    def __iter__(self) -> Iterator[int]:
        for row in self.values():
            yield row[self.metadata['primary_key']]

    def __len__(self) -> int:
        return self.metadata['count']

    def values(self) -> Iterator[dict]:
        for number in range(BUCKETS):
            yield from self.store.scan_bucket(self.metadata, number)

    def items(self) -> Iterator[tuple[int, dict]]:
        for row in self.values():
            yield row[self.metadata['primary_key']], row


class Store:
    def __init__(self, root: Path, cache_bytes: int = 32 * 1024 * 1024):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self.objects = root / 'objects'
        self.objects.mkdir(exist_ok=True)
        self.scratch = root / 'scratch'
        self.scratch.mkdir(exist_ok=True)
        self.catalog_path = root / 'catalog.json'
        self.cache_bytes = cache_bytes
        self.cache: OrderedDict[str, tuple[list[dict], int]] = OrderedDict()
        self.used_cache = 0
        self.created: set[str] = set()
        self.transaction: dict | None = None
        if self.catalog_path.exists():
            envelope = json.loads(self.catalog_path.read_bytes())
            self.committed = envelope['catalog']
            payload = self._catalog_bytes(self.committed)
            if envelope['sha256'] != hashlib.sha256(payload).hexdigest():
                raise DatabaseError('storage_corrupt', 'catalog checksum mismatch')
        else:
            self.committed = {}
        self._recover()

    @staticmethod
    def _catalog_bytes(catalog: dict) -> bytes:
        return json.dumps(catalog, ensure_ascii=True, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()

    @property
    def catalog(self) -> dict:
        return self.transaction if self.transaction is not None else self.committed

    @staticmethod
    def references(catalog: dict) -> set[str]:
        return {name for table in catalog.values() for name in table['buckets'].values()}

    def _recover(self) -> None:
        live = self.references(self.committed)
        for name in live:
            if not (self.objects / name).is_file():
                raise DatabaseError('storage_corrupt', 'committed page missing')
        # Cleanup is idempotent: a second kill during recovery cannot touch
        # anything the committed catalog references.
        for entry in self.objects.iterdir():
            if entry.name not in live and entry.is_file():
                entry.unlink()
        for entry in self.scratch.iterdir():
            if entry.is_file():
                entry.unlink()
        pending = self.root / 'catalog.pending'
        pending.unlink(missing_ok=True)

    def begin(self) -> None:
        if self.transaction is not None:
            raise DatabaseError('transaction_error', 'nested transactions are unsupported')
        self.transaction = copy.deepcopy(self.committed)

    def rollback(self) -> None:
        self.transaction = None
        self._collect()

    def commit(self) -> None:
        if self.transaction is None:
            return
        candidate = self.transaction
        self._publish(candidate)
        self.transaction = None
        self._collect()

    def _finish(self, catalog: dict) -> None:
        if self.transaction is not None:
            self.transaction = catalog
        else:
            self._publish(catalog)
        self._collect()

    def _publish(self, catalog: dict) -> None:
        live = self.references(catalog)
        for name in live & self.created:
            with (self.objects / name).open('rb') as stream:
                os.fsync(stream.fileno())
        sync_directory(self.objects)
        payload = self._catalog_bytes(catalog)
        envelope = {'catalog': catalog, 'sha256': hashlib.sha256(payload).hexdigest()}
        pending = self.root / 'catalog.pending'
        with pending.open('wb') as stream:
            stream.write(json.dumps(envelope, separators=(',', ':'), allow_nan=False).encode())
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pending, self.catalog_path)
        sync_directory(self.root)
        self.committed = catalog
        self.created.difference_update(live)

    def _collect(self) -> None:
        live = self.references(self.committed) | self.references(self.catalog)
        # Published pages replaced by a newer catalog are also eligible. There
        # are at most BUCKETS pages per table plus the current statement/tx.
        for entry in self.objects.iterdir():
            if entry.name not in live and entry.is_file():
                entry.unlink(missing_ok=True)
                self.created.discard(entry.name)
                cached = self.cache.pop(entry.name, None)
                if cached:
                    self.used_cache -= cached[1]

    def metadata(self, name: str) -> dict:
        try:
            return self.catalog[name]
        except KeyError as error:
            raise DatabaseError('table_not_found', f'unknown table {name}') from error

    def create(self, name: str, columns: list[dict], primary_key: str, vector_column: str | None) -> None:
        if name in self.catalog:
            raise DatabaseError('table_exists', f'table {name} already exists')
        candidate = copy.deepcopy(self.catalog)
        candidate[name] = {'columns': columns, 'primary_key': primary_key, 'vector_column': vector_column,
                           'indexes': {}, 'buckets': {}, 'count': 0}
        self._finish(candidate)

    def create_index(self, name: str, index: str, column: str) -> None:
        metadata = self.metadata(name)
        if metadata['vector_column'] != column:
            raise DatabaseError('invalid_vector_index', 'index must target the vector column')
        if index in metadata['indexes']:
            raise DatabaseError('index_exists', 'index already exists')
        candidate = copy.deepcopy(self.catalog)
        candidate[name]['indexes'][index] = column
        self._finish(candidate)

    @staticmethod
    def encode(metadata: dict, row: dict) -> bytes:
        parts = []
        for column in metadata['columns']:
            value = row[column['name']]
            if column['kind'] == 'BIGINT':
                parts.append(_I64.pack(value))
            elif column['kind'] == 'VARCHAR':
                value = value.encode('utf-8')
                parts.extend((_U32.pack(len(value)), value))
            else:
                parts.append(np.asarray(value, dtype='<f4').tobytes())
        payload = b''.join(parts)
        return _U32.pack(len(payload)) + payload

    @staticmethod
    def decode(metadata: dict, payload: bytes) -> dict:
        offset, row = 0, {}
        for column in metadata['columns']:
            kind = column['kind']
            if kind == 'BIGINT':
                value = _I64.unpack_from(payload, offset)[0]
                offset += 8
            elif kind == 'VARCHAR':
                length = _U32.unpack_from(payload, offset)[0]
                offset += 4
                value = payload[offset:offset+length].decode('utf-8')
                offset += length
            else:
                length = column['length']
                value = np.frombuffer(payload, dtype='<f4', count=length, offset=offset)
                offset += length * 4
            row[column['name']] = value
        if offset != len(payload):
            raise DatabaseError('storage_corrupt', 'invalid page record')
        return row

    def _read_page(self, metadata: dict, filename: str) -> Iterator[dict]:
        with (self.objects / filename).open('rb') as stream:
            while prefix := stream.read(4):
                if len(prefix) != 4:
                    raise DatabaseError('storage_corrupt', 'truncated record length')
                length = _U32.unpack(prefix)[0]
                payload = stream.read(length)
                if len(payload) != length:
                    raise DatabaseError('storage_corrupt', 'truncated record')
                yield self.decode(metadata, payload)

    def scan_bucket(self, metadata: dict, number: int) -> Iterator[dict]:
        filename = metadata['buckets'].get(str(number))
        if filename is None:
            return
        if filename in self.cache:
            rows, _ = self.cache[filename]
            self.cache.move_to_end(filename)
            yield from rows
            return
        size = (self.objects / filename).stat().st_size
        # Account for dict/string/array wrapper overhead, not only payload.
        estimate = size * 3 + 1024 * (metadata['count'] // max(1, len(metadata['buckets'])) + 1)
        if estimate <= self.cache_bytes // 4:
            rows = list(self._read_page(metadata, filename))
            weight = size * 3 + len(rows) * 1024
            while self.cache and self.used_cache + weight > self.cache_bytes:
                _, (_, removed) = self.cache.popitem(last=False)
                self.used_cache -= removed
            if weight <= self.cache_bytes:
                self.cache[filename] = rows, weight
                self.used_cache += weight
            yield from rows
        else:
            # Oversized partitions are streamed. Correctness does not depend
            # on fitting a bucket, a table, or the full database in memory.
            yield from self._read_page(metadata, filename)

    def _new_page(self, metadata: dict, rows: Iterator[dict]) -> tuple[str | None, int]:
        filename = uuid.uuid4().hex + '.page'
        path = self.objects / filename
        self.created.add(filename)
        count = 0
        with path.open('xb') as stream:
            for row in rows:
                stream.write(self.encode(metadata, row))
                count += 1
        if not count:
            path.unlink()
            self.created.remove(filename)
            return None, 0
        return filename, count

    def insert(self, name: str, incoming: list[dict]) -> None:
        metadata = self.metadata(name)
        key_column = metadata['primary_key']
        grouped: dict[int, dict[int, dict]] = {}
        for row in incoming:
            key = row[key_column]
            group = grouped.setdefault(bucket(key), {})
            if key in group:
                raise DatabaseError('duplicate_key', 'duplicate primary key in statement')
            group[key] = row
        candidate = copy.deepcopy(self.catalog)
        try:
            for number, additions in grouped.items():
                def combined():
                    for old in self.scan_bucket(metadata, number):
                        if old[key_column] in additions:
                            raise DatabaseError('duplicate_key', 'duplicate primary key')
                        yield old
                    yield from additions.values()
                filename, _ = self._new_page(metadata, combined())
                candidate[name]['buckets'][str(number)] = filename
            candidate[name]['count'] += len(incoming)
            self._finish(candidate)
        except DatabaseError:
            self._collect()
            raise

    def rewrite(self, name: str, change: Callable[[dict], tuple[dict | None, bool]]) -> int:
        metadata = self.metadata(name)
        candidate = copy.deepcopy(self.catalog)
        affected, removed = 0, 0
        try:
            for number in range(BUCKETS):
                if str(number) not in metadata['buckets']:
                    continue
                bucket_changed = False
                def transformed():
                    nonlocal affected, removed, bucket_changed
                    for row in self.scan_bucket(metadata, number):
                        new, matched = change(row)
                        if matched:
                            affected += 1
                            bucket_changed = True
                        if new is None:
                            removed += 1
                        else:
                            yield new
                filename, _ = self._new_page(metadata, transformed())
                if bucket_changed:
                    if filename:
                        candidate[name]['buckets'][str(number)] = filename
                    else:
                        candidate[name]['buckets'].pop(str(number), None)
                elif filename:
                    (self.objects / filename).unlink()
                    self.created.remove(filename)
            candidate[name]['count'] -= removed
            if affected:
                self._finish(candidate)
            else:
                self._collect()
            return affected
        except DatabaseError:
            self._collect()
            raise
