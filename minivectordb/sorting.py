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

"""Bounded external merge sort; temporary runs live inside the data directory."""
from __future__ import annotations

import heapq
import pickle
import tempfile
from pathlib import Path


class Descending:
    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        return self.value > other.value

    def __eq__(self, other):
        return isinstance(other, Descending) and self.value == other.value


def external_sort(source, key, scratch: Path, budget: int = 4 * 1024 * 1024):
    files = set()

    def write_run(rows):
        with tempfile.NamedTemporaryFile(dir=scratch, suffix='.sort', delete=False) as stream:
            path = Path(stream.name)
            files.add(path)
            for row in rows:
                pickle.dump(row, stream, protocol=5)
        return path

    def read_run(path):
        with path.open('rb') as stream:
            while True:
                try:
                    yield pickle.load(stream)
                except EOFError:
                    return

    try:
        runs, buffer, size = [], [], 0
        for row in source:
            buffer.append(row)
            size += len(pickle.dumps(row, protocol=5)) + 512
            if size >= budget:
                runs.append(write_run(sorted(buffer, key=key)))
                buffer, size = [], 0
        if not runs:
            yield from sorted(buffer, key=key)
            return
        if buffer:
            runs.append(write_run(sorted(buffer, key=key)))
        while len(runs) > 32:
            merged = []
            for offset in range(0, len(runs), 32):
                group = runs[offset:offset+32]
                merged.append(write_run(heapq.merge(*(read_run(path) for path in group), key=key)))
                for path in group:
                    path.unlink()
                    files.remove(path)
            runs = merged
        yield from heapq.merge(*(read_run(path) for path in runs), key=key)
    finally:
        for path in files:
            path.unlink(missing_ok=True)
