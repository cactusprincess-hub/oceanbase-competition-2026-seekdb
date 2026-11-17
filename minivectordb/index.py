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

"""A bounded flat vector index over immutable page versions.

The index stores float32 matrices and row identities, never precomputed query
answers. Replacing a page gives it a new identity, so old index blocks cannot
be reused for a modified transaction snapshot. ANN requests use this exact
fallback; no external vector-index package is used.
"""
from __future__ import annotations

from collections import OrderedDict

import numpy as np

from .storage import BUCKETS


class FlatIndex:
    def __init__(self, store, budget=16 * 1024 * 1024):
        self.store = store
        self.budget = budget
        self.cache = OrderedDict()
        self.used = 0

    def blocks(self, metadata):
        column = metadata['vector_column']
        if column is None:
            return
        for number in range(BUCKETS):
            filename = metadata['buckets'].get(str(number))
            if filename is None:
                continue
            cached = self.cache.get(filename)
            if cached:
                self.cache.move_to_end(filename)
                rows, matrix, _ = cached
                yield rows, matrix
                continue
            rows = []
            weight = 0
            blocks = []
            can_cache = True
            for row in self.store.scan_bucket(metadata, number):
                rows.append(row)
                weight += len(self.store.encode(metadata, row)) * 3 + 1024
                if len(rows) >= 256 or weight >= 1024 * 1024:
                    matrix = np.asarray([item[column] for item in rows], dtype=np.float32)
                    yield rows, matrix
                    # Multiple blocks or large payloads stream without retaining
                    # the partition. Typical small pages use the cache below.
                    can_cache = False
                    rows, weight = [], 0
            if rows:
                matrix = np.asarray([item[column] for item in rows], dtype=np.float32)
                cost = weight + matrix.nbytes
                if can_cache and cost <= self.budget:
                    while self.cache and self.used + cost > self.budget:
                        _, (_, _, old_cost) = self.cache.popitem(last=False)
                        self.used -= old_cost
                    self.cache[filename] = (rows, matrix, cost)
                    self.used += cost
                yield rows, matrix

    def prepare(self, metadata):
        for _ in self.blocks(metadata):
            pass
