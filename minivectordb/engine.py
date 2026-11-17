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

from __future__ import annotations

import json
import heapq
import itertools
import operator
import math
import re
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from collections.abc import Iterable

import sqlglot
import numpy as np
from sqlglot import exp

from minivectordb.errors import DatabaseError, unsupported
from minivectordb.storage import RowView, Store
from minivectordb.sorting import Descending, external_sort
from minivectordb.index import FlatIndex


@dataclass(frozen=True)
class Column:
    name: str
    kind: str
    length: int | None = None


@dataclass
class Table:
    columns: list[Column]
    primary_key: str
    vector_column: str | None
    rows: RowView
    indexes: set[str]


@dataclass
class Result:
    columns: list[str] | None = None
    rows: Iterable[list[Any]] | None = None
    affected_rows: int | None = None


class Engine:
    def __init__(self, data_dir: str | Path):
        self.store = Store(Path(data_dir))
        self.index = FlatIndex(self.store)
        # Rebuild index blocks for persisted declarations before ready. Oversized
        # pages use bounded streaming blocks rather than a whole-table matrix.
        for metadata in self.store.catalog.values():
            if metadata['indexes']:
                self.index.prepare(metadata)

    @property
    def tables(self) -> dict[str, Table]:
        return self._view()

    def execute(self, sql: str, search_mode: str = "exact") -> Result:
        try:
            parsed = sqlglot.parse(sql, read="mysql")
        except sqlglot.errors.ParseError as error:
            raise unsupported(str(error)) from error
        if len(parsed) != 1 or parsed[0] is None:
            raise unsupported("one SQL statement is required")
        statement = parsed[0]
        index_match = re.fullmatch(
            r"\s*CREATE\s+VECTOR\s+INDEX\s+`?([A-Za-z_][A-Za-z0-9_]*)`?\s+ON\s+`?([A-Za-z_][A-Za-z0-9_]*)`?\s*\(\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\s*\)\s*;?\s*",
            sql,
            re.IGNORECASE,
        )
        if index_match:
            index_name, table_name, column_name = (value.lower() for value in index_match.groups())
            table = self._table(self._view(), table_name)
            if table.vector_column != column_name:
                raise DatabaseError("invalid_vector_index", "index must target the vector column")
            self.index.prepare(self.store.metadata(table_name))
            self.store.create_index(table_name, index_name, column_name)
            return Result()
        if isinstance(statement, exp.Create):
            self._check_clauses(statement, {'this', 'kind'})
            return self._create(statement)
        if isinstance(statement, exp.Insert):
            self._check_clauses(statement, {'this', 'expression'})
            return self._insert(statement)
        if isinstance(statement, exp.Select):
            return self._select(statement, search_mode)
        if isinstance(statement, exp.Update):
            self._check_clauses(statement, {'this', 'expressions', 'where'})
            return self._update(statement)
        if isinstance(statement, exp.Delete):
            self._check_clauses(statement, {'this', 'where'})
            return self._delete(statement)
        command = sql.strip().rstrip(";").strip().lower()
        if command in {"begin", "begin transaction", "start transaction"}:
            self.store.begin()
            return Result()
        if command in {"commit", "commit work"}:
            self.commit()
            return Result()
        if command in {"rollback", "rollback work"}:
            self.rollback()
            return Result()
        raise unsupported()

    @staticmethod
    def _check_clauses(statement, allowed):
        if any(value and key not in allowed for key, value in statement.args.items()):
            raise unsupported('unsupported SQL clause')

    def commit(self) -> None:
        self.store.commit()

    def rollback(self) -> None:
        self.store.rollback()

    def _view(self) -> dict[str, Table]:
        return {name: Table(
            columns=[Column(**c) for c in meta['columns']],
            primary_key=meta['primary_key'], vector_column=meta['vector_column'],
            rows=RowView(self.store, meta), indexes=set(meta['indexes']),
        ) for name, meta in self.store.catalog.items()}

    def _create(self, statement: exp.Create) -> Result:
        if statement.args.get("kind") != "TABLE" or not isinstance(statement.this, exp.Schema):
            raise unsupported()
        schema = statement.this
        table_name = self._name(schema.this)
        columns: list[Column] = []
        primary_key = None
        vector_column = None
        for item in schema.expressions:
            if not isinstance(item, exp.ColumnDef):
                if isinstance(item, exp.PrimaryKey):
                    if primary_key is not None:
                        raise unsupported('only one primary key is permitted')
                    keys = item.expressions
                    if len(keys) != 1:
                        raise unsupported("one primary key is required")
                    primary_key = self._name(keys[0])
                    continue
                raise unsupported("unsupported table constraint")
            name = self._name(item.this)
            data_type = item.kind
            if not isinstance(data_type, exp.DataType):
                raise unsupported("invalid column type")
            kind_name = data_type.this.value.upper() if hasattr(data_type.this, "value") else str(data_type.this).upper()
            length = None
            if kind_name == "VARCHAR":
                values = data_type.expressions
                length_node = values[0].this if values else None
                if not isinstance(length_node, exp.Literal):
                    raise unsupported("VARCHAR length required")
                length = int(length_node.this)
            elif kind_name == "VECTOR":
                values = data_type.expressions
                length_node = values[0].this if values else None
                if not isinstance(length_node, exp.Literal):
                    raise unsupported("VECTOR dimension required")
                length = int(length_node.this)
                if vector_column is not None:
                    raise unsupported("one vector column per table")
                vector_column = name
                kind_name = "VECTOR"
            elif kind_name not in {"BIGINT"}:
                raise unsupported(f"unsupported type {kind_name}")
            if name in {column.name for column in columns}:
                raise unsupported('duplicate column name')
            if length is not None and length <= 0:
                raise unsupported('length or dimension must be positive')
            if data_type.args.get('unsigned'):
                raise unsupported('unsigned types are unsupported')
            columns.append(Column(name, kind_name, length))
            for constraint in item.args.get("constraints", []):
                if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                    if primary_key is not None:
                        raise unsupported("one primary key is required")
                    primary_key = name
                elif not isinstance(constraint.kind, exp.NotNullColumnConstraint):
                    raise unsupported('unsupported column constraint')
        if primary_key is None or primary_key not in {column.name for column in columns}:
            raise unsupported("BIGINT primary key required")
        if next(column for column in columns if column.name == primary_key).kind != "BIGINT":
            raise unsupported("primary key must be BIGINT")
        self.store.create(table_name, [column.__dict__ for column in columns], primary_key, vector_column)
        return Result()

    def _insert(self, statement: exp.Insert) -> Result:
        if not isinstance(statement.expression, exp.Values):
            raise unsupported("INSERT VALUES required")
        schema = statement.this if isinstance(statement.this, exp.Schema) else None
        table_expr = schema.this if schema else statement.this
        table_name = self._name(table_expr)
        table = self._table(self._view(), table_name)
        names = [self._name(column) for column in schema.expressions] if schema else [c.name for c in table.columns]
        if len(names) != len(set(names)):
            raise unsupported('duplicate INSERT columns')
        if names != [column.name for column in table.columns]:
            if set(names) != {column.name for column in table.columns}:
                raise unsupported("INSERT must provide all columns")
        rows = []
        for value_row in statement.expression.expressions:
            values = value_row.expressions
            if len(values) != len(names):
                raise DatabaseError("column_count", "column count does not match")
            raw = {name: self._literal(value) for name, value in zip(names, values, strict=True)}
            row = self._coerce_row(table, raw)
            rows.append(row)
        self.store.insert(table_name, rows)
        return Result(affected_rows=len(rows))

    def _update(self, statement: exp.Update) -> Result:
        table_name = self._name(statement.this)
        table = self._table(self._view(), table_name)
        assignments = {}
        for assignment in statement.expressions:
            if not isinstance(assignment, exp.EQ) or not isinstance(assignment.this, exp.Column):
                raise unsupported("simple assignments required")
            assignments[self._name(assignment.this)] = self._literal(assignment.expression)
        if not assignments or set(assignments) - {column.name for column in table.columns}:
            raise unsupported("unknown update column")
        if table.primary_key in assignments:
            raise unsupported("primary key updates are unsupported")
        predicate = self._condition(statement.args.get('where'), table)
        assignments = {name: self._coerce_value(next(c for c in table.columns if c.name == name), value)
                       for name, value in assignments.items()}
        def update(row):
            if not predicate(row):
                return row, False
            proposed = dict(row)
            proposed.update(assignments)
            return self._coerce_row(table, proposed), True
        return Result(affected_rows=self.store.rewrite(table_name, update))

    def _delete(self, statement: exp.Delete) -> Result:
        table_name = self._name(statement.this)
        table = self._table(self._view(), table_name)
        predicate = self._condition(statement.args.get('where'), table)
        return Result(affected_rows=self.store.rewrite(
            table_name, lambda row: (None, True) if predicate(row) else (row, False)))

    def _select(self, statement: exp.Select, search_mode: str) -> Result:
        if search_mode not in {"exact", "ann"}:
            raise unsupported("invalid search mode")
        allowed = {'expressions', 'from', 'where', 'order', 'limit'}
        if any(value and name not in allowed for name, value in statement.args.items()):
            raise unsupported("unsupported SELECT clause")
        from_clause = statement.args.get('from')
        table_expr = from_clause.this if from_clause else None
        if not isinstance(table_expr, exp.Table):
            raise unsupported("one table is required")
        table_name = self._name(table_expr)
        table = self._table(self._view(), table_name)
        predicate = self._condition(statement.args.get('where'), table)
        known = {column.name: column for column in table.columns}
        distances = {}
        def compile_expression(expression):
            if isinstance(expression, exp.Column):
                name = self._name(expression)
                if name not in known:
                    raise unsupported(f"unknown column {name}")
                return lambda record: record[0][name]
            distance = self._distance_projection(expression)
            if distance:
                (column, vector), _ = distance
                if column != table.vector_column:
                    raise DatabaseError('invalid_vector_column', 'distance requires a vector column')
                if len(vector) != known[column].length:
                    raise DatabaseError('dimension_mismatch', 'query vector dimension does not match')
                identity = expression.sql(dialect='mysql')
                distances[identity] = (column, np.asarray(vector, dtype=np.float64))
                return lambda record: record[1][identity]
            raise unsupported('unsupported projection or sort expression')

        names, getters, aliases = [], [], {}
        for projection in statement.expressions:
            value = projection.this if isinstance(projection, exp.Alias) else projection
            if isinstance(value, exp.Star) or isinstance(value, exp.Column) and isinstance(value.this, exp.Star):
                if isinstance(projection, exp.Alias):
                    raise unsupported('star alias is unsupported')
                for column in table.columns:
                    names.append(column.name)
                    getters.append(lambda record, name=column.name: record[0][name])
                continue
            getter = compile_expression(value)
            label = projection.alias if isinstance(projection, exp.Alias) else (
                self._name(value) if isinstance(value, exp.Column) else 'L2_DISTANCE')
            names.append(label)
            getters.append(getter)
            aliases[label.lower()] = getter
        orders = []
        for ordered in (statement.args['order'].expressions if statement.args.get('order') else []):
            expression = ordered.this
            getter = aliases.get(expression.name.lower()) if isinstance(expression, exp.Column) else None
            if getter is None:
                getter = compile_expression(expression)
            if isinstance(expression, exp.Column) and expression.name.lower() in known and known[expression.name.lower()].kind == 'VECTOR':
                raise unsupported('vector ORDER BY requires L2_DISTANCE')
            orders.append((getter, bool(ordered.args.get('desc'))))
        count = None
        if statement.args.get('limit'):
            count = self._literal(statement.args['limit'].expression)
            if type(count) is not int or count < 0:
                raise DatabaseError('invalid_limit', 'LIMIT must be a nonnegative integer')
        def sort_key(record):
            keys = tuple(Descending(getter(record)) if desc else getter(record) for getter, desc in orders)
            return keys + (record[0][table.primary_key],)
        def evaluate():
            if not distances:
                yield from ((row, {}) for row in table.rows.values() if predicate(row))
                return
            metadata = self.store.metadata(table_name)
            for rows, float32_matrix in self.index.blocks(metadata):
                mask = np.asarray([predicate(row) for row in rows], dtype=bool)
                positions = np.flatnonzero(mask)
                if not len(positions):
                    continue
                matrix = float32_matrix[mask].astype(np.float64)
                scores = {}
                for identity, (_, query) in distances.items():
                    difference = matrix - query
                    scores[identity] = np.sqrt(np.einsum('ij,ij->i', difference, difference))
                for index, position in enumerate(positions):
                    yield rows[int(position)], {identity: float(values[index]) for identity, values in scores.items()}
        if count == 0:
            chosen = []
        elif orders and count is not None and count <= 1024:
            chosen = heapq.nsmallest(count, evaluate(), key=sort_key)
        elif orders:
            chosen = external_sort(evaluate(), sort_key, self.store.scratch)
            if count is not None:
                chosen = itertools.islice(chosen, count)
        else:
            chosen = evaluate()
            if count is not None:
                chosen = itertools.islice(chosen, count)
        def project():
            try:
                for record in chosen:
                    yield [value.tolist() if isinstance(value, np.ndarray) else value
                           for value in (getter(record) for getter in getters)]
            finally:
                if hasattr(chosen, 'close'):
                    chosen.close()
        return Result(columns=names, rows=project())

    def _coerce_value(self, column: Column, value: Any) -> Any:
        if column.kind == 'BIGINT':
            if type(value) is not int or not -(2**63) <= value < 2**63:
                raise DatabaseError('invalid_integer', f'{column.name} requires signed BIGINT')
        elif column.kind == 'VARCHAR':
            if not isinstance(value, str):
                raise DatabaseError('invalid_varchar', f'{column.name} requires VARCHAR')
            if len(value) > (column.length or 0):
                raise DatabaseError('string_too_long', f'{column.name} exceeds VARCHAR length')
            try:
                value.encode('utf-8')
            except UnicodeError as error:
                raise DatabaseError('invalid_varchar', 'invalid Unicode string') from error
        elif column.kind == 'VECTOR':
            if isinstance(value, np.ndarray):
                return value
            value = self._vector(value, column.length)
        return value

    def _coerce_row(self, table: Table, values: dict[str, Any]) -> dict[str, Any]:
        if set(values) != {column.name for column in table.columns}:
            raise DatabaseError('column_count', 'row must contain every column')
        return {column.name: self._coerce_value(column, values[column.name]) for column in table.columns}

    def _condition(self, where: exp.Where | None, table: Table):
        columns = {column.name: column for column in table.columns}
        comparisons = {exp.EQ: operator.eq, exp.GT: operator.gt, exp.GTE: operator.ge,
                       exp.LT: operator.lt, exp.LTE: operator.le}
        def compile_condition(node):
            if isinstance(node, exp.Paren):
                return compile_condition(node.this)
            if isinstance(node, exp.And):
                left, right = compile_condition(node.this), compile_condition(node.expression)
                return lambda row: left(row) and right(row)
            comparison = comparisons.get(type(node))
            if comparison is None or not isinstance(node.this, exp.Column):
                raise unsupported('condition must use column comparisons and AND')
            name = self._name(node.this)
            if name not in columns:
                raise unsupported(f'unknown condition column {name}')
            if columns[name].kind == 'VECTOR':
                raise unsupported('vector comparisons require distance queries')
            value = self._literal(node.expression)
            if columns[name].kind == 'BIGINT' and type(value) is not int:
                raise unsupported('BIGINT condition requires an integer')
            if columns[name].kind == 'VARCHAR' and not isinstance(value, str):
                raise unsupported('VARCHAR condition requires a string')
            return lambda row: comparison(row[name], value)
        return compile_condition(where.this) if where else lambda row: True

    def _distance_projection(self, expression: exp.Expression) -> tuple[tuple[str, list[float]], str] | None:
        value = expression.this if isinstance(expression, exp.Alias) else expression
        if not isinstance(value, exp.Anonymous) or value.name.upper() != "L2_DISTANCE" or len(value.expressions) != 2:
            return None
        if not isinstance(value.expressions[0], exp.Column):
            raise unsupported("L2_DISTANCE requires a vector column")
        column = self._name(value.expressions[0])
        vector = self._vector(self._literal(value.expressions[1]), None)
        return ((column, vector), expression.alias_or_name if isinstance(expression, exp.Alias) else "L2_DISTANCE")

    @staticmethod
    def _distance(left: list[float], right: list[float]) -> float:
        return math.sqrt(sum((a - b) * (a - b) for a, b in zip(left, right, strict=True)))

    @staticmethod
    def _vector(value: Any, dimension: int | None) -> list[float]:
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError as error:
                raise DatabaseError("invalid_vector", "vector must be a JSON array") from error
        if not isinstance(value, list) or (dimension is not None and len(value) != dimension):
            raise DatabaseError("dimension_mismatch", "vector dimension does not match")
        result = []
        for item in value:
            if isinstance(item, bool) or not isinstance(item, (float, int)):
                raise DatabaseError("invalid_vector", "vector values must be numeric")
            try:
                number = struct.unpack("!f", struct.pack("!f", float(item)))[0]
            except (OverflowError, struct.error) as error:
                raise DatabaseError("invalid_vector", "vector value exceeds float32") from error
            if not math.isfinite(number):
                raise DatabaseError("invalid_vector", "vector values must be finite")
            result.append(number)
        return result

    @staticmethod
    def _literal(expression: exp.Expression) -> Any:
        if isinstance(expression, exp.Literal):
            if expression.is_string:
                return expression.this
            try:
                return int(expression.this) if re.fullmatch(r'[0-9]+', expression.this) else float(expression.this)
            except ValueError as error:
                raise unsupported('invalid numeric literal') from error
        if isinstance(expression, exp.Neg):
            value = Engine._literal(expression.this)
            if not isinstance(value, (int, float)):
                raise unsupported("invalid numeric literal")
            return -value
        if isinstance(expression, exp.Array):
            return [Engine._literal(value) for value in expression.expressions]
        raise unsupported("only literal values are supported")

    @staticmethod
    def _name(expression: exp.Expression) -> str:
        if not isinstance(expression, (exp.Identifier, exp.Column, exp.Table)):
            raise unsupported("invalid identifier")
        if isinstance(expression, exp.Table) and (expression.args.get('db') or expression.args.get('catalog')):
            raise unsupported('qualified database names are unsupported')
        return str(expression.name if hasattr(expression, "name") else expression.this).lower()

    @staticmethod
    def _table(tables: dict[str, Table], name: str) -> Table:
        try:
            return tables[name]
        except KeyError as error:
            raise DatabaseError("table_not_found", f"unknown table {name}") from error
